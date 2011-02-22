{- | A pool of TCP connections to a single server or a replica set of servers. -}

{-# LANGUAGE CPP, OverloadedStrings, ScopedTypeVariables, RecordWildCards, NamedFieldPuns, MultiParamTypeClasses, FlexibleContexts, TypeFamilies, DoRec, RankNTypes, FlexibleInstances #-}

module Database.MongoDB.Connection (
	-- * Pipe
	Pipe,
	-- * Host
	Host(..), PortID(..), host, showHostPort, readHostPort, readHostPortM,
	-- * ReplicaSet
	ReplicaSet(..), Name,
	-- * MasterOrSlaveOk
	MasterOrSlaveOk(..),
	-- * Connection Pool
	Service(..),
	connHost, replicaSet
) where

import Database.MongoDB.Internal.Protocol as X
import qualified Network.Abstract as C
import Network.Abstract (IOE, NetworkIO, ANetwork)
import Data.Bson ((=:), at, UString)
import Control.Pipeline as P
import Control.Applicative ((<$>))
import Control.Exception (assert)
import Control.Monad.Error
import Control.Monad.MVar
import Network (HostName, PortID(..))
import Data.Bson (Document, look)
import Text.ParserCombinators.Parsec as T (parse, many1, letter, digit, char, eof, spaces, try, (<|>))
import Control.Monad.Identity
import Control.Monad.Util (MonadIO', untilSuccess)
import Database.MongoDB.Internal.Util ()  -- PortID instances
import Var.Pool
import System.Random (newStdGen, randomRs)
import Data.List (delete, find, nub)
import System.IO.Unsafe (unsafePerformIO)

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
-- TODO: Distinguish Service and UnixSocket port
showHostPort (Host hostname port) = hostname ++ ":" ++ portname  where
	portname = case port of
		Service s -> s
		PortNumber p -> show p
#if !defined(mingw32_HOST_OS) && !defined(cygwin32_HOST_OS) && !defined(_WIN32)
		UnixSocket s -> s
#endif

readHostPortM :: (Monad m) => String -> m Host
-- ^ Read string \"hostname:port\" as @Host hosthame port@ or \"hostname\" as @host hostname@ (default port). Fail if string does not match either syntax.
-- TODO: handle Service and UnixSocket port
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

getReplicaInfo :: Pipe -> IOE ReplicaInfo
-- ^ Get replica info of the connected host. Throw IOError if connection fails or host is not part of a replica set (no /hosts/ and /primary/ field).
getReplicaInfo pipe = do
	promise <- X.call pipe [] (adminCommand ["ismaster" =: (1 :: Int)])
	info <- commandReply "ismaster" <$> promise
	_ <- look "hosts" info
	_ <- look "primary" info
	return info

type ReplicaInfo = Document
-- ^ Configuration info of a host in a replica set (result of /ismaster/ command). Contains all the hosts in the replica set plus its role in that set (master, slave, or arbiter)

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

-- * Connection Pool

type Pool' = Pool IOError

-- | A Service is a single server ('Host') or a replica set of servers ('ReplicaSet')
class Service t where
	data ConnPool t
	-- ^ A pool of TCP connections ('Pipe's) to a host or a replica set of hosts
	newConnPool :: (NetworkIO m) => Int -> t -> m (ConnPool t)
	-- ^ Create a ConnectionPool to a host or a replica set of hosts. Actual TCP connection is not attempted until 'getPipe' request, so no IOError can be raised here. Up to N TCP connections will be established to each host.
	getPipe :: MasterOrSlaveOk -> ConnPool t -> IOE Pipe
	-- ^ Return a TCP connection (Pipe) to the master or a slave in the server. Master must connect to the master, SlaveOk may connect to a slave or master. To spread the load, SlaveOk requests are distributed amongst all hosts in the server. Throw IOError if failed to connect to right type of host (Master/SlaveOk).
	killPipes :: ConnPool t -> IO ()
	-- ^ Kill all open pipes (TCP Connections). Will cause any users of them to fail. Alternatively you can let them die on their own when they get garbage collected.

-- ** ConnectionPool Host

instance Service Host where
	data ConnPool Host = HostConnPool {connHost :: Host, connPool :: Pool' Pipe}
	-- ^ A pool of TCP connections ('Pipe's) to a server, handed out in round-robin style.
	newConnPool poolSize' host' = liftIO . newHostConnPool poolSize' host' =<< C.network
	-- ^ Create a connection pool to server (host or replica set)
	getPipe _ = getHostPipe
	-- ^ Return a TCP connection (Pipe). If SlaveOk, connect to a slave if available. Round-robin if multiple slaves are available. Throw IOError if failed to connect.
	killPipes (HostConnPool _ pool) = killAll pool

instance Show (ConnPool Host) where
	show HostConnPool{connHost} = "ConnPool " ++ show connHost

newHostConnPool :: Int -> Host -> ANetwork -> IO (ConnPool Host)
-- ^ Create a pool of N 'Pipe's (TCP connections) to server. 'getHostPipe' will return one of those pipes, round-robin style.
newHostConnPool poolSize' host' net = HostConnPool host' <$> newPool Factory{..} poolSize' where
	newResource = tcpConnect net host'
	killResource = P.close
	isExpired = P.isClosed

getHostPipe :: ConnPool Host -> IOE Pipe
-- ^ Return next pipe (TCP connection) in connection pool, round-robin style. Throw IOError if can't connect to host.
getHostPipe (HostConnPool _ pool) = aResource pool

tcpConnect :: ANetwork -> Host -> IOE Pipe
-- ^ Create a TCP connection (Pipe) to the given host. Throw IOError if can't connect.
tcpConnect net (Host hostname port) = newPipeline =<< C.connect net (C.Server hostname port)

-- ** Connection ReplicaSet

instance Service ReplicaSet where
	data ConnPool ReplicaSet = ReplicaSetConnPool {
		network :: ANetwork,
		repsetName :: Name,
		currentMembers :: MVar [ConnPool Host] }  -- master at head after a refresh
	newConnPool poolSize' repset = liftIO . newSetConnPool poolSize' repset =<< C.network
	getPipe = getSetPipe
	killPipes ReplicaSetConnPool{..} = withMVar currentMembers (mapM_ killPipes)

instance Show (ConnPool ReplicaSet) where
	show r = "ConnPool " ++ show (unsafePerformIO $ replicaSet r)

replicaSet :: (MonadIO' m) => ConnPool ReplicaSet -> m ReplicaSet
-- ^ Return replicas set name with current members as seed list
replicaSet ReplicaSetConnPool{..} = ReplicaSet repsetName . map connHost <$> readMVar currentMembers

newSetConnPool :: Int -> ReplicaSet -> ANetwork -> IO (ConnPool ReplicaSet)
-- ^ Create a connection pool to each member of the replica set.
newSetConnPool poolSize' repset net = assert (not . null $ seedHosts repset) $ do
	currentMembers <- newMVar =<< mapM (\h -> newHostConnPool poolSize' h net) (seedHosts repset)
	return $ ReplicaSetConnPool net (setName repset) currentMembers

getMembers :: Name -> [ConnPool Host] -> IOE [Host]
-- ^ Get members of replica set, master first. Query supplied connections until config found.
-- TODO: Verify config for request replica set name and not some other replica set. ismaster config should include replica set name in result but currently does not.
getMembers _repsetName connections = hosts <$> untilSuccess (getReplicaInfo <=< getHostPipe) connections

refreshMembers :: ANetwork -> Name -> [ConnPool Host] -> IOE [ConnPool Host]
-- ^ Update current members with master at head. Reuse unchanged members. Throw IOError if can't connect to any and fetch config. Dropped connections are not closed in case they still have users; they will be closed when garbage collected.
refreshMembers net repsetName connections = do
	n <- liftIO . poolSize . connPool $ head connections
	mapM (liftIO . connection n) =<< getMembers repsetName connections
 where
	connection n host' = maybe (newHostConnPool n host' net) return mc  where
		mc = find ((host' ==) . connHost) connections
		

getSetPipe :: MasterOrSlaveOk -> ConnPool ReplicaSet -> IOE Pipe
-- ^ Return a pipe to primary or a random secondary in replica set. Use primary for SlaveOk if and only if no secondaries. Note, refreshes members each time (makes ismaster call to primary).
getSetPipe mos ReplicaSetConnPool{..} = modifyMVar currentMembers $ \conns -> do
	connections <- refreshMembers network repsetName conns  -- master at head after refresh
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
