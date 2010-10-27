{- | A Mongo connection is a pool of TCP connections to a single server or a replica set of servers. -}

{-# LANGUAGE OverloadedStrings, ScopedTypeVariables, RecordWildCards, MultiParamTypeClasses, FlexibleContexts, TypeFamilies, DoRec, RankNTypes #-}

module Database.MongoDB.Connection (
	-- * Host
	Host(..), PortID(..), host, showHostPort, readHostPort, readHostPortM,
	-- * ReplicaSet
	ReplicaSet(..),
	-- * MasterOrSlaveOk
	MasterOrSlaveOk(..),
	-- * Connection
	Server(..), replicaSet
) where

import Database.MongoDB.Internal.Protocol
import Data.Bson ((=:), at, UString)
import Control.Pipeline (Resource(..))
import Control.Applicative ((<$>))
import Control.Exception (assert)
import System.IO.Error as E (try)
import Control.Monad.Error
import Control.Monad.MVar
import Network (HostName, PortID(..), connectTo)
import Data.Bson (Document, look)
import Text.ParserCombinators.Parsec as T (parse, many1, letter, digit, char, eof, spaces, try, (<|>))
import Control.Monad.Identity
import Control.Monad.Util (MonadIO', untilSuccess)
import Database.MongoDB.Internal.Util ()  -- PortID instances
import Var.Pool
import System.Random (newStdGen, randomRs)
import Data.List (delete, find, nub)

type Name = UString

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

-- * Replica Set

data ReplicaSet = ReplicaSet {setName :: Name, seedHosts :: [Host]}  deriving (Show)
-- ^ Replica set of hosts identified by set name. At least one of the seed hosts must be an active member of the set. However, this list is not used to identify the set, just the set name.

instance Eq ReplicaSet where ReplicaSet x _ == ReplicaSet y _ = x == y

-- ** Replica Info

getReplicaInfo :: Pipe -> ErrorT IOError IO ReplicaInfo
-- ^ Get replica info of the connected host. Throw IOError if connection fails or host is not part of a replica set (no /hosts/ and /primary/ field).
getReplicaInfo pipe = do
	promise <- call pipe [] (adminCommand ["ismaster" =: (1 :: Int)])
	info <- commandReply "ismaster" <$> promise
	_ <- look "hosts" info
	_ <- look "primary" info
	return info

type ReplicaInfo = Document
-- ^ Configuration info of a host in a replica set. Contains all the hosts in the replica set plus its role in that set (master, slave, or arbiter)

{- isPrimary :: ReplicaInfo -> Bool
-- ^ Is the replica described by this info a master/primary (not slave or arbiter)?
isPrimary = true1 "ismaster"

isSecondary :: ReplicaInfo -> Bool
-- ^ Is the replica described by this info a slave/secondary (not master or arbiter)
isSecondary = true1 "secondary" -}

replicas :: ReplicaInfo -> [Host]
-- ^ All replicas in set according to this replica configuration info.
replicas = map readHostPort . at "hosts"

primary :: ReplicaInfo -> Host
-- ^ Read primary from configuration info
primary = readHostPort . at "primary"

hosts :: ReplicaInfo -> [Host]
-- ^ replicas with primary at head
hosts info = master : delete master members  where
	members = replicas info
	master = primary info

-- * MasterOrSlaveOk

data MasterOrSlaveOk =
	  Master  -- ^ connect to master only
	| SlaveOk  -- ^ connect to a slave, or master if no slave available
	deriving (Show, Eq)

{- isMS :: MasterOrSlaveOk -> ReplicaInfo -> Bool
-- ^ Does the host (as described by its replica-info) match the master/slave type
isMS Master i = isPrimary i
isMS SlaveOk i = isSecondary i || isPrimary i -}

-- * Connection

type Pool' = Pool IOError

-- | A Server is a single server ('Host') or a replica set of servers ('ReplicaSet')
class Server t where
	data Connection t
	-- ^ A Mongo connection is a pool of TCP connections to a host or a replica set of hosts
	connect :: (MonadIO' m) => Int -> t -> m (Connection t)
	-- ^ Create a Mongo Connection to a host or a replica set of hosts. Actual TCP connection is not attempted until 'getPipe' request, so no IOError can be raised here. Up to N TCP connections will be established to each host.
	getPipe :: MasterOrSlaveOk -> Connection t -> ErrorT IOError IO Pipe
	-- ^ Return a TCP connection (Pipe) to the master or a slave in the server. Master must connect to the master, SlaveOk may connect to a slave or master. To spread the load, SlaveOk requests are distributed amongst all hosts in the server. Throw IOError if failed to connect to right type of host (Master/SlaveOk).
	killPipes :: Connection t -> IO ()
	-- ^ Kill all open pipes (TCP Connections). Will cause any users of them to fail. Alternatively you can let them die on their own when this Connection is garbage collected.

-- ** Connection Host

instance Server Host where
	data Connection Host = HostConnection {connHost :: Host, connPool :: Pool' Pipe}
	-- ^ A pool of TCP connections ('Pipe's) to a server, handed out in round-robin style.
	connect poolSize' host' = liftIO (connectHost poolSize' host')
	-- ^ Create a Connection (pool of TCP connections) to server (host or replica set)
	getPipe _ = getHostPipe
	-- ^ Return a TCP connection (Pipe). If SlaveOk, connect to a slave if available. Round-robin if multiple slaves are available. Throw IOError if failed to connect.
	killPipes (HostConnection _ pool) = killAll pool

connectHost :: Int -> Host -> IO (Connection Host)
-- ^ Create a pool of N 'Pipe's (TCP connections) to server. 'getHostPipe' will return one of those pipes, round-robin style.
connectHost poolSize' host' = HostConnection host' <$> newPool Factory{..} poolSize' where
	newResource = tcpConnect host'
	killResource = close
	isExpired = isClosed

getHostPipe :: Connection Host -> ErrorT IOError IO Pipe
-- ^ Return next pipe (TCP connection) in connection pool, round-robin style. Throw IOError if can't connect to host.
getHostPipe (HostConnection _ pool) = aResource pool

tcpConnect :: Host -> ErrorT IOError IO Pipe
-- ^ Create a TCP connection (Pipe) to the given host. Throw IOError if can't connect.
tcpConnect (Host hostname port) = ErrorT . E.try $ mkPipe =<< connectTo hostname port

-- ** Connection ReplicaSet

instance Server ReplicaSet where
	data Connection ReplicaSet = ReplicaSetConnection {
		repsetName :: Name,
		currentMembers :: MVar [Connection Host] }  -- master at head after a refresh
	connect poolSize' repset = liftIO (connectSet poolSize' repset)
	getPipe = getSetPipe
	killPipes ReplicaSetConnection{..} = withMVar currentMembers (mapM_ killPipes)

replicaSet :: (MonadIO' m) => Connection ReplicaSet -> m ReplicaSet
-- ^ Set name with current members as seed list
replicaSet ReplicaSetConnection{..} = ReplicaSet repsetName . map connHost <$> readMVar currentMembers

connectSet :: Int -> ReplicaSet -> IO (Connection ReplicaSet)
-- ^ Create a connection to each member of the replica set.
connectSet poolSize' repset = assert (not . null $ seedHosts repset) $ do
	currentMembers <- newMVar =<< mapM (connect poolSize') (seedHosts repset)
	return $ ReplicaSetConnection (setName repset) currentMembers

getMembers :: Name -> [Connection Host] -> ErrorT IOError IO [Host]
-- ^ Get members of replica set, master first. Query supplied connections until config found.
-- TODO: Verify config for request replica set name and not some other replica set. ismaster config should include replica set name in result but currently does not.
getMembers _repsetName connections = hosts <$> untilSuccess (getReplicaInfo <=< getHostPipe) connections

refreshMembers :: Name -> [Connection Host] -> ErrorT IOError IO [Connection Host]
-- ^ Update current members with master at head. Reuse unchanged members. Throw IOError if can't connect to any and fetch config. Dropped connections are not closed in case they still have users; they will be closed when garbage collected.
refreshMembers repsetName connections = do
	n <- liftIO . poolSize . connPool $ head connections
	mapM (connection n) =<< getMembers repsetName connections
 where
	connection n host' = maybe (connect n host') return $ find ((host' ==) . connHost) connections

getSetPipe :: MasterOrSlaveOk -> Connection ReplicaSet -> ErrorT IOError IO Pipe
-- ^ Return a pipe to primary or a random secondary in replica set. Use primary for SlaveOk if and only if no secondaries. Note, refreshes members each time (makes ismaster call to primary).
getSetPipe mos ReplicaSetConnection{..} = modifyMVar currentMembers $ \conns -> do
	connections <- refreshMembers repsetName conns  -- master at head after refresh
	pipe <- case mos of
		Master -> getHostPipe (head connections)
		SlaveOk -> do
			let n = length connections - 1
			is <- take (max 1 n) . nub . randomRs (min 1 n, n) <$> liftIO newStdGen
			untilSuccess (getHostPipe . (connections !!)) is
	return (connections, pipe)


{- Authors: Tony Hannan <tony@10gen.com>
   Copyright 2010 10gen Inc.
   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. -}
