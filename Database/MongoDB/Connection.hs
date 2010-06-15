{- | A replica set is a set of servers that mirror each other (a non-replicated server can act like a replica set of one). One server in a replica set is the master and the rest are slaves. When the master goes down, one of the slaves becomes master. The ReplicaSet object in this client maintains a list of servers that it currently knows are in the set. It refreshes this list every time it establishes a new connection with one of the servers in the set. Each server in the set knows who the other member in the set are, and who is master. The user asks the ReplicaSet object for a new master or slave connection. When a connection fails, the user must ask the ReplicaSet for a new connection (which most likely will connect to another server since the previous one failed). When you loose a connection you loose all session state that was stored with that connection on the server, which includes open cursors and temporary map-reduce output collections. Attempting to read from a lost cursor (on a new connection) will only returning the remaining documents in the last batch returned to this client. It will not fetch the remaining documents from the server. Likewise, attempting to read a lost map-reduce output will return an empty set of documents. Notice, in both cases, no error is raised, just empty results. -}

{-# LANGUAGE OverloadedStrings, ScopedTypeVariables #-}

module Database.MongoDB.Connection (
	-- * Server
	I.Server(..), PortID(..), server, showHostPort, readHostPort, readHostPortF,
	-- * ReplicaSet
	ReplicaSet, replicaSet, replicaServers,
	MasterOrSlave(..), FailedToConnect, newConnection,
	-- * Connection
	I.Connection, I.connServer, I.showHandle,
	connect, I.closeConnection, I.isClosed,
	-- * Connected monad
	I.Conn(..), I.Failure(..),
	-- ** Task
	I.Task, I.runTask,
	-- ** Op
	I.Op
) where

import Database.MongoDB.Internal.Connection as I
import Database.MongoDB.Query (useDb, runCommand1)
import Control.Applicative ((<$>))
import Control.Arrow ((+++), left)
import Control.Exception (assert)
import System.IO.Error as E (try)
import Control.Monad.Error
import Data.IORef
import Network (HostName, PortID(..), connectTo)
import Data.Bson (Document, look, typed)
import Text.ParserCombinators.Parsec as P (parse, many1, letter, digit, char, eof, spaces, try, (<|>))
import Control.Monad.Identity
import Database.MongoDB.Util (true1)  -- PortID instances

-- * Server

defaultPort :: PortID
defaultPort = PortNumber 27017

server :: HostName -> Server
-- ^ Server on default MongoDB port
server host = Server host defaultPort

showHostPort :: Server -> String
-- ^ Display server as \"host:port\"
showHostPort (Server host port) = host ++ ":" ++ (case port of
	Service s -> s
	PortNumber p -> show p
	UnixSocket s -> s)

readHostPortF :: (Monad m) => String -> m Server
-- ^ Read string \"host:port\" as 'Server host port' or \"host\" as 'server host' (default port). Fail if string does not match either syntax.
readHostPortF = either (fail . show) return . parse parser "readHostPort" where
	hostname = many1 (letter <|> digit <|> char '-' <|> char '.')
	parser = do
		spaces
		host <- hostname
		P.try (spaces >> eof >> return (server host)) <|> do
			_ <- char ':'
			port :: Int <- read <$> many1 digit
			spaces >> eof
			return $ Server host (PortNumber $ fromIntegral port)

readHostPort :: String -> Server
-- ^ Read string \"host:port\" as 'Server host port' or \"host\" as 'server host' (default port). Error if string does not match either syntax.
readHostPort = runIdentity . readHostPortF

-- * Replica Set

newtype ReplicaSet = ReplicaSet (IORef [Server])
-- ^ Reference to a replica set of servers. Ok if really not a replica set and just a stand-alone server, in which case it acts like a replica set of one.

replicaSet :: [Server] -> IO ReplicaSet
-- ^ Create a reference to a replica set with servers as the initial seed list (a subset of the servers in the replica set)
replicaSet s = assert (not $ null s) (ReplicaSet <$> newIORef s)

replicaServers :: ReplicaSet -> IO [Server]
-- ^ Return current list of known servers in replica set. This list is updated on every 'newConnection'.
replicaServers (ReplicaSet ref) = readIORef ref

-- * Replica Info

data ReplicaInfo = ReplicaInfo Server Document  deriving (Show, Eq)
-- ^ Configuration info of a server in a replica set. Contains all the servers in the replica set plus its role in that set (master, slave, or arbiter)

isMaster :: ReplicaInfo -> Bool
-- ^ Is the replica server described by this info a master/primary (not slave or arbiter)?
isMaster (ReplicaInfo _ i) = true1 "ismaster" i

isSlave :: ReplicaInfo -> Bool
-- ^ Is the replica server described by this info a slave/secondary (not master or arbiter)
isSlave = not . isMaster  -- TODO: distinguish between slave and arbiter

allReplicas :: ReplicaInfo -> [Server]
-- ^ All replicas in set according to this replica configuration info.
-- If server is stand-alone then it won't have \"hosts\" in it configuration, in which case we return the server by itself.
allReplicas (ReplicaInfo s i) = maybe [s] (map readHostPort . typed) (look "hosts" i)

sortedReplicas :: ReplicaInfo -> IO [Server]
-- ^ All replicas in set sorted by distance from this client. TODO
sortedReplicas = return . allReplicas

getReplicaInfo' :: Connection -> IO (Either IOError ReplicaInfo)
-- ^ Get replica info of the connected server. Return Left IOError if connection fails
getReplicaInfo' conn = left err <$> runTask getReplicaInfo conn  where
	err (ConnectionFailure e) = e
	err (ServerFailure s) = userError s

getReplicaInfo :: (Conn m) => m ReplicaInfo
-- ^ Get replica info of connect server
getReplicaInfo = do
	c <- getConnection
	ReplicaInfo (connServer c) <$> useDb "admin" (runCommand1 "ismaster")

-- * MasterOrSlave

data MasterOrSlave =
	  Master  -- ^ connect to master only
	| SlaveOk  -- ^ connect to a slave, or master if no slave available
	deriving (Show, Eq)

isMS :: MasterOrSlave -> ReplicaInfo -> Bool
-- ^ Does the server (as described by its info) match the master/slave type
isMS Master i = isMaster i
isMS SlaveOk i = isSlave i || isMaster i

-- * Connection

type FailedToConnect = [(Server, IOError)]
-- ^ All servers tried in replica set along with reason why each failed to connect

newConnection :: MasterOrSlave -> ReplicaSet -> IO (Either FailedToConnect Connection)
-- ^ Create a connection to a master or slave in the replica set. Don't forget to close connection when you are done using it even if Failure exception is raised when using it. newConnection returns Left if failed to connect to any server in replica set.
-- TODO: prefer slave over master when SlaveOk and both are available.
newConnection mos (ReplicaSet vServers) = do
	servers <- readIORef vServers
	e <- connectFirst mos servers
	case e of
		Right (conn, info) -> do
			writeIORef vServers =<< sortedReplicas info
			return (Right conn)
		Left (fs, is) -> if null is
			then return (Left fs)
			else do
				replicas <- sortedReplicas (head is)
				writeIORef vServers replicas
				(fst +++ fst) <$> connectFirst mos replicas

connectFirst :: MasterOrSlave -> [Server] -> IO (Either ([(Server, IOError)], [ReplicaInfo]) (Connection, ReplicaInfo))
-- ^ Connect to first server that succeeds and is master/slave, otherwise return list of failed connections plus info of connections that succeeded but were not master/slave.
connectFirst mos = connectFirst' ([], []) where
	connectFirst' (fs, is) [] = return $ Left (fs, is)
	connectFirst' (fs, is) (s : ss) = do
		e <- runErrorT $ do
			c <- ErrorT (connect s)
			i <- ErrorT (getReplicaInfo' c)
			return (c, i)
		case e of
			Left f -> connectFirst' ((s, f) : fs, is) ss
			Right (c, i) -> if isMS mos i
				then return $ Right (c, i)
				else do
					closeConnection c
					connectFirst' ((s, userError $ "not a " ++ show mos) : fs, i : is) ss

connect :: Server -> IO (Either IOError Connection)
-- ^ Create a connection to the given server (as opposed to connecting to some server in a replica set via 'newConnection'). Return Left IOError if failed to connect.
connect s@(Server host port) = E.try (mkConnection s =<< connectTo host port)


{- Authors: Tony Hannan <tony@10gen.com>
   Copyright 2010 10gen Inc.
   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. -}
