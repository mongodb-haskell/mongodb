-- | Connect to a single server or a replica set of servers

{-# LANGUAGE CPP, OverloadedStrings, ScopedTypeVariables, TupleSections #-}

module Database.MongoDB.Connection (
	IOE, runIOE,
	-- * Connection
	Pipe, close, isClosed,
	-- * Host
	Host(..), PortID(..), host, showHostPort, readHostPort, readHostPortM, connect,
	-- * Replica Set
	ReplicaSetName, openReplicaSet, ReplicaSet, primary, secondaryOk
) where

import Prelude hiding (lookup)
import Database.MongoDB.Internal.Protocol (Pipe, writeMessage, readMessage)
import System.IO.Pipeline (IOE, IOStream(..), newPipeline, close, isClosed)
import System.IO.Error as E (try)
import System.IO (hClose)
import Network (HostName, PortID(..), connectTo)
import Text.ParserCombinators.Parsec as T (parse, many1, letter, digit, char, eof, spaces, try, (<|>))
import Control.Monad.Identity (runIdentity)
import Control.Monad.Error (ErrorT(..), lift, throwError)
import Control.Monad.MVar
import Control.Monad (forM_)
import Control.Applicative ((<$>))
import Data.UString (UString, unpack)
import Data.Bson as D (Document, lookup, at, (=:))
import Database.MongoDB.Query (access, safe, MasterOrSlaveOk(SlaveOk), Failure(ConnectionFailure), Command, runCommand)
import Database.MongoDB.Internal.Util (untilSuccess, liftIOE, runIOE, updateAssocs, shuffle)
import Data.List as L (lookup, intersect, partition, (\\))

adminCommand :: Command -> Pipe -> IOE Document
-- ^ Run command against admin database on server connected to pipe. Fail if connection fails.
adminCommand cmd pipe =
	liftIOE failureToIOError . ErrorT $ access pipe safe SlaveOk "admin" $ runCommand cmd
 where
	failureToIOError (ConnectionFailure e) = e
	failureToIOError e = userError $ show e

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

connect :: Host -> IOE Pipe
-- ^ Connect to Host returning pipelined TCP connection. Throw IOError if problem connecting.
connect (Host hostname port) = do
	handle <- ErrorT . E.try $ connectTo hostname port
	lift $ newPipeline $ IOStream (writeMessage handle) (readMessage handle) (hClose handle)

-- * Replica Set

type ReplicaSetName = UString

-- | Maintains a connection (created on demand) to each server in the named replica set
data ReplicaSet = ReplicaSet ReplicaSetName (MVar [(Host, Maybe Pipe)])

openReplicaSet :: (ReplicaSetName, [Host]) -> IOE ReplicaSet
-- ^ Open connections (on demand) to servers in replica set. Supplied hosts is seed list. At least one of them must be a live member of the named replica set, otherwise fail.
openReplicaSet (rsName, seedList) = do
	rs <- ReplicaSet rsName <$> newMVar (map (, Nothing) seedList)
	_ <- updateMembers rs
	return rs

primary :: ReplicaSet -> IOE Pipe
-- ^ Return connection to current primary of replica set
primary rs@(ReplicaSet rsName _) = do
	mHost <- statedPrimary <$> updateMembers rs
	case mHost of
		Just host' -> connection rs Nothing host'
		Nothing -> throwError $ userError $ "replica set " ++ unpack rsName ++ " has no primary"

secondaryOk :: ReplicaSet -> IOE Pipe
-- ^ Return connection to a random member (secondary or primary)
secondaryOk rs = do
	info <- updateMembers rs
	hosts <- lift $ shuffle (possibleHosts info)
	untilSuccess (connection rs Nothing) hosts

type ReplicaInfo = (Host, Document)
-- ^ Result of isMaster command on host in replica set. Returned fields are: setName, ismaster, secondary, hosts, [primary]. primary only present when ismaster = false

statedPrimary :: ReplicaInfo -> Maybe Host
-- ^ Primary of replica set or Nothing if there isn't one
statedPrimary (host', info) = if (at "ismaster" info) then Just host' else readHostPort <$> D.lookup "primary" info

possibleHosts :: ReplicaInfo -> [Host]
-- ^ Non-arbiter, non-hidden members of replica set
possibleHosts (_, info) = map readHostPort $ at "hosts" info

updateMembers :: ReplicaSet -> IOE ReplicaInfo
-- ^ Fetch replica info from any server and update members accordingly
updateMembers rs@(ReplicaSet _ vMembers) = do
	(host', info) <- untilSuccess (fetchReplicaInfo rs) =<< readMVar vMembers
	modifyMVar vMembers $ \members -> do
		let ((members', old), new) = intersection (map readHostPort $ at "hosts" info) members
		lift $ forM_ old $ \(_, mPipe) -> maybe (return ()) close mPipe
		return (members' ++ map (, Nothing) new, (host', info))
 where
	intersection :: (Eq k) => [k] -> [(k, v)] -> (([(k, v)], [(k, v)]), [k])
	intersection keys assocs = (partition (flip elem inKeys . fst) assocs, keys \\ inKeys) where
		assocKeys = map fst assocs
		inKeys = intersect keys assocKeys

fetchReplicaInfo :: ReplicaSet -> (Host, Maybe Pipe) -> IOE ReplicaInfo
-- Connect to host and fetch replica info from host creating new connection if missing or closed (previously failed). Fail if not member of named replica set.
fetchReplicaInfo rs@(ReplicaSet rsName _) (host', mPipe) = do
	pipe <- connection rs mPipe host'
	info <- adminCommand ["isMaster" =: (1 :: Int)] pipe
	case D.lookup "setName" info of
		Nothing -> throwError $ userError $ show host' ++ " not a member of any replica set, including " ++ unpack rsName ++ ": " ++ show info
		Just setName | setName /= rsName -> throwError $ userError $ show host' ++ " not a member of replica set " ++ unpack rsName ++ ": " ++ show info
		Just _ -> return (host', info)

connection :: ReplicaSet -> Maybe Pipe -> Host -> IOE Pipe
-- ^ Return new or existing connection to member of replica set. If pipe is already known for host it is given, but we still test if it is open.
connection (ReplicaSet _ vMembers) mPipe host' =
	maybe conn (\p -> lift (isClosed p) >>= \bad -> if bad then conn else return p) mPipe
 where
 	conn = 	modifyMVar vMembers $ \members -> do
		let new = connect host' >>= \pipe -> return (updateAssocs host' (Just pipe) members, pipe)
		case L.lookup host' members of
			Just (Just pipe) -> lift (isClosed pipe) >>= \bad -> if bad then new else return (members, pipe)
			_ -> new


{- Authors: Tony Hannan <tony@10gen.com>
   Copyright 2011 10gen Inc.
   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. -}
