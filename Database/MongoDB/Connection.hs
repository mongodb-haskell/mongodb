{- | A replica set is a set of servers that mirror each other (a non-replicated server can act like a replica set of one). One server in a replica set is the master and the rest are slaves. When the master goes down, one of the slaves becomes master. The ReplicaSet object in this client maintains a list of servers that it currently knows are in the set. It refreshes this list every time it establishes a new connection with one of the servers in the set. Each server in the set knows who the other member in the set are, and who is master. The user asks the ReplicaSet object for a new master or slave connection. When a connection fails, the user must ask the ReplicaSet for a new connection (which most likely will connect to another server since the previous one failed). When connecting to a new server you loose all session state that was stored with the old server, which includes open cursors and temporary map-reduce output collections. Attempting to read from a lost cursor on a new server will raise a ServerFailure exception. Attempting to read a lost map-reduce temp output on a new server will return an empty set (not an error, like it maybe should). -}

{-# LANGUAGE OverloadedStrings, ScopedTypeVariables, RecordWildCards, MultiParamTypeClasses, FlexibleContexts #-}

module Database.MongoDB.Connection (
	runNet,
	-- * Host
	Host(..), PortID(..), host, showHostPort, readHostPort, readHostPortM,
	-- * ReplicaSet
	ReplicaSet, replicaSet, replicas,
	newConnection,
	-- * MasterOrSlaveOk
	MasterOrSlaveOk(..),
	-- * Connection
	Connection, connect,
	-- * Resource
	Resource(..)
) where

import Database.MongoDB.Internal.Protocol
import Data.Bson ((=:), at)
import Control.Pipeline (Resource(..))
import Control.Applicative ((<$>))
import Control.Arrow ((+++), left)
import Control.Exception (assert)
import System.IO.Error as E (try)
import Control.Monad.Error
import Control.Monad.Throw
import Data.IORef
import Network (HostName, PortID(..), connectTo)
import Data.Bson (Document, look, typed)
import Text.ParserCombinators.Parsec as T (parse, many1, letter, digit, char, eof, spaces, try, (<|>))
import Control.Monad.Identity
import Database.MongoDB.Internal.Util (true1, MonadIO')  -- PortID instances

runNet :: ErrorT IOError m a -> m (Either IOError a)
-- ^ Execute action that raises IOError only on network problem. Other IOErrors like file access errors are not caught by this.
runNet = runErrorT

adminCommand :: Document -> Request
-- ^ Convert command to request
adminCommand cmd = Query{..} where
	qOptions = [SlaveOK]
	qFullCollection = "admin.$cmd"
	qSkip = 0
	qBatchSize = 0
	qSelector = cmd
	qProjector = []

commandReply :: String -> Reply -> Document
-- ^ Extract first document from reply. Error if query error, using given string as prefix error message.
commandReply title Reply{..} = if elem QueryError rResponseFlags
	then error $ title ++ ": " ++ at "$err" (head rDocuments)
	else head rDocuments

-- * Host

data Host = Host HostName PortID  deriving (Show, Eq, Ord)

defaultPort :: PortID
defaultPort = PortNumber 27017

host :: HostName -> Host
-- ^ Host on default MongoDB port
host hostname = Host hostname defaultPort

showHostPort :: Host -> String
-- ^ Display host as \"host:port\"
showHostPort (Host hostname port) = hostname ++ ":" ++ (case port of
	Service s -> s
	PortNumber p -> show p
	UnixSocket s -> s)

readHostPortM :: (Monad m) => String -> m Host
-- ^ Read string \"hostname:port\" as @Host hosthame port@ or \"hostname\" as @host hostname@ (default port). Fail if string does not match either syntax.
readHostPortM = either (fail . show) return . parse parser "readHostPort" where
	hostname = many1 (letter <|> digit <|> char '-' <|> char '.')
	parser = do
		spaces
		h <- hostname
		T.try (spaces >> eof >> return (host h)) <|> do
			_ <- char ':'
			port :: Int <- read <$> many1 digit
			spaces >> eof
			return $ Host h (PortNumber $ fromIntegral port)

readHostPort :: String -> Host
-- ^ Read string \"hostname:port\" as @Host hostname port@ or \"hostname\" as @host hostname@ (default port). Error if string does not match either syntax.
readHostPort = runIdentity . readHostPortM

-- ** Replica Set

newtype ReplicaSet = ReplicaSet (IORef [Host])
-- ^ Reference to a replica set of hosts. Ok if really not a replica set and just a stand-alone server, in which case it acts like a replica set of one.

replicaSet :: [Host] -> IO ReplicaSet
-- ^ Create a reference to a replica set with given hosts as the initial seed list (a subset of the hosts in the replica set)
replicaSet s = assert (not $ null s) (ReplicaSet <$> newIORef s)

replicas :: ReplicaSet -> IO [Host]
-- ^ Return current list of known hosts in replica set. This list is updated on every 'newConnection'.
replicas (ReplicaSet ref) = readIORef ref

-- * Replica Info

data ReplicaInfo = ReplicaInfo Host Document  deriving (Show, Eq)
-- ^ Configuration info of a host in a replica set. Contains all the hosts in the replica set plus its role in that set (master, slave, or arbiter)

isMaster :: ReplicaInfo -> Bool
-- ^ Is the replica described by this info a master/primary (not slave or arbiter)?
isMaster (ReplicaInfo _ i) = true1 "ismaster" i

isSlave :: ReplicaInfo -> Bool
-- ^ Is the replica described by this info a slave/secondary (not master or arbiter)
isSlave = not . isMaster  -- TODO: distinguish between slave and arbiter

allReplicas :: ReplicaInfo -> [Host]
-- ^ All replicas in set according to this replica configuration info.
-- If host is stand-alone then it won't have \"hosts\" in its configuration, in which case we return the host by itself.
allReplicas (ReplicaInfo h i) = maybe [h] (map readHostPort . typed) (look "hosts" i)

sortedReplicas :: ReplicaInfo -> IO [Host]
-- ^ All replicas in set sorted by distance from this client. TODO
sortedReplicas = return . allReplicas

getReplicaInfo :: (Throw IOError m, MonadIO' m) => Host -> Connection -> m ReplicaInfo
-- ^ Get replica info of the connected host. Throw IOError if connection fails.
getReplicaInfo host' conn = do
	promise <- throwLeft . liftIO . E.try $ call conn [] (adminCommand ["ismaster" =: (1 :: Int)])
	fmap (ReplicaInfo host' . commandReply "ismaster") . throwLeft . liftIO $ E.try promise

-- * MasterOrSlaveOk

data MasterOrSlaveOk =
	  Master  -- ^ connect to master only
	| SlaveOk  -- ^ connect to a slave, or master if no slave available
	deriving (Show, Eq)

isMS :: MasterOrSlaveOk -> ReplicaInfo -> Bool
-- ^ Does the host (as described by its replica-info) match the master/slave type
isMS Master i = isMaster i
isMS SlaveOk i = isSlave i || isMaster i

-- * Connection

newConnection :: (Throw IOError m, MonadIO' m) => MasterOrSlaveOk -> ReplicaSet -> m Connection
-- ^ Create a connection to a master or slave in the replica set. Throw IOError if failed to connect to any host in replica set that is the right master/slave type. 'close' connection when you are done using it even if a failure is raised. Garbage collected connections will be closed automatically (but don't rely on this when creating many connections).
-- TODO: prefer slave over master when SlaveOk and both are available.
newConnection mos (ReplicaSet vHosts) = throwLeft . liftIO $ left (userError . show) <$> do
	hosts <- readIORef vHosts
	e <- connectFirst mos hosts
	case e of
		Right (conn, info) -> do
			writeIORef vHosts =<< sortedReplicas info
			return (Right conn)
		Left (fs, is) -> if null is
			then return (Left fs)
			else do
				replicas <- sortedReplicas (head is)
				writeIORef vHosts replicas
				-- try again in case new replicas in info
				(fst +++ fst) <$> connectFirst mos replicas

connectFirst :: MasterOrSlaveOk -> [Host] -> IO (Either ([(Host, IOError)], [ReplicaInfo]) (Connection, ReplicaInfo))
-- ^ Connect to first host that succeeds and is master/slave, otherwise return list of failed connections plus info of connections that succeeded but were not master/slave.
connectFirst mos = connectFirst' ([], []) where
	connectFirst' (fs, is) [] = return $ Left (fs, is)
	connectFirst' (fs, is) (h : hs) = do
		e <- runErrorT $ do
			c <- connect h
			i <- getReplicaInfo h c
			return (c, i)
		case e of
			Left f -> connectFirst' ((h, f) : fs, is) hs
			Right (c, i) -> if isMS mos i
				then return $ Right (c, i)
				else do
					close c
					connectFirst' ((h, userError $ "not a " ++ show mos) : fs, i : is) hs

connect :: (Throw IOError m, MonadIO' m) => Host -> m Connection
-- ^ Create a connection to the given host (as opposed to connecting to some host in a replica set via 'newConnection'). Throw IOError if can't connect.
connect (Host hostname port) = throwLeft . liftIO $ E.try (mkConnection =<< connectTo hostname port)


{- Authors: Tony Hannan <tony@10gen.com>
   Copyright 2010 10gen Inc.
   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. -}
