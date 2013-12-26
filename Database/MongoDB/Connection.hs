-- | Connect to a single server or a replica set of servers

{-# LANGUAGE CPP, OverloadedStrings, ScopedTypeVariables, TupleSections #-}

module Database.MongoDB.Connection (
    -- * Util
    Secs,
    -- * Connection
    Pipe, close, isClosed,
    -- * Server
    Host(..), PortID(..), defaultPort, host, showHostPort, readHostPort,
    readHostPortM, globalConnectTimeout, connect, connect',
    -- * Replica Set
    ReplicaSetName, openReplicaSet, openReplicaSet',
    ReplicaSet, primary, secondaryOk, routedHost, closeReplicaSet, replSetName
) where

import Prelude hiding (lookup)
import Data.IORef (IORef, newIORef, readIORef)
import Data.List (intersect, partition, (\\), delete)
import Control.Applicative ((<$>))
import Control.Monad (forM_)
import Network (HostName, PortID(..), connectTo)
import System.IO.Unsafe (unsafePerformIO)
import System.Timeout (timeout)
import Text.ParserCombinators.Parsec (parse, many1, letter, digit, char, eof,
                                      spaces, try, (<|>))
import qualified Data.List as List


import Control.Monad.Identity (runIdentity)
import Control.Monad.Error (throwError)
import Control.Concurrent.MVar.Lifted (MVar, newMVar, withMVar, modifyMVar,
                                       readMVar)
import Data.Bson (Document, at, (=:))
import Data.Text (Text)

import qualified Data.Bson as B
import qualified Data.Text as T

import Database.MongoDB.Internal.Protocol (Pipe, newPipe)
import Database.MongoDB.Internal.Util (untilSuccess, liftIOE,
                                       updateAssocs, shuffle, mergesortM)
import Database.MongoDB.Query (Command, Failure(ConnectionFailure), access,
                              slaveOk, runCommand)
import System.IO.Pipeline (close, isClosed)

adminCommand :: Command -> Pipe -> IO Document
-- ^ Run command against admin database on server connected to pipe. Fail if connection fails.
adminCommand cmd pipe =
    liftIOE failureToIOError $ access pipe slaveOk "admin" $ runCommand cmd
 where
    failureToIOError (ConnectionFailure e) = e
    failureToIOError e = userError $ show e

-- * Host

data Host = Host HostName PortID  deriving (Show, Eq, Ord)

defaultPort :: PortID
-- ^ Default MongoDB port = 27017
defaultPort = PortNumber 27017

host :: HostName -> Host
-- ^ Host on 'defaultPort'
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
-- ^ Read string \"hostname:port\" as @Host hosthame (PortNumber port)@ or \"hostname\" as @host hostname@ (default port). Fail if string does not match either syntax.
-- TODO: handle Service and UnixSocket port
readHostPortM = either (fail . show) return . parse parser "readHostPort" where
    hostname = many1 (letter <|> digit <|> char '-' <|> char '.')
    parser = do
        spaces
        h <- hostname
        try (spaces >> eof >> return (host h)) <|> do
            _ <- char ':'
            port :: Int <- read <$> many1 digit
            spaces >> eof
            return $ Host h (PortNumber $ fromIntegral port)

readHostPort :: String -> Host
-- ^ Read string \"hostname:port\" as @Host hostname (PortNumber port)@ or \"hostname\" as @host hostname@ (default port). Error if string does not match either syntax.
readHostPort = runIdentity . readHostPortM

type Secs = Double

globalConnectTimeout :: IORef Secs
-- ^ 'connect' (and 'openReplicaSet') fails if it can't connect within this many seconds (default is 6 seconds). Use 'connect\'' (and 'openReplicaSet\'') if you want to ignore this global and specify your own timeout. Note, this timeout only applies to initial connection establishment, not when reading/writing to the connection.
globalConnectTimeout = unsafePerformIO (newIORef 6)
{-# NOINLINE globalConnectTimeout #-}

connect :: Host -> IO Pipe
-- ^ Connect to Host returning pipelined TCP connection. Throw IOError if connection refused or no response within 'globalConnectTimeout'.
connect h = readIORef globalConnectTimeout >>= flip connect' h

connect' :: Secs -> Host -> IO Pipe
-- ^ Connect to Host returning pipelined TCP connection. Throw IOError if connection refused or no response within given number of seconds.
connect' timeoutSecs (Host hostname port) = do
    mh <- timeout (round $ timeoutSecs * 1000000) (connectTo hostname port)
    handle <- maybe (ioError $ userError "connect timed out") return mh
    newPipe handle

-- * Replica Set

type ReplicaSetName = Text

-- | Maintains a connection (created on demand) to each server in the named replica set
data ReplicaSet = ReplicaSet ReplicaSetName (MVar [(Host, Maybe Pipe)]) Secs

replSetName :: ReplicaSet -> Text
-- ^ name of connected replica set
replSetName (ReplicaSet rsName _ _) = rsName

openReplicaSet :: (ReplicaSetName, [Host]) -> IO ReplicaSet
-- ^ Open connections (on demand) to servers in replica set. Supplied hosts is seed list. At least one of them must be a live member of the named replica set, otherwise fail. The value of 'globalConnectTimeout' at the time of this call is the timeout used for future member connect attempts. To use your own value call 'openReplicaSet\'' instead.
openReplicaSet rsSeed = readIORef globalConnectTimeout >>= flip openReplicaSet' rsSeed

openReplicaSet' :: Secs -> (ReplicaSetName, [Host]) -> IO ReplicaSet
-- ^ Open connections (on demand) to servers in replica set. Supplied hosts is seed list. At least one of them must be a live member of the named replica set, otherwise fail. Supplied seconds timeout is used for connect attempts to members.
openReplicaSet' timeoutSecs (rsName, seedList) = do
    vMembers <- newMVar (map (, Nothing) seedList)
    let rs = ReplicaSet rsName vMembers timeoutSecs
    _ <- updateMembers rs
    return rs

closeReplicaSet :: ReplicaSet -> IO ()
-- ^ Close all connections to replica set
closeReplicaSet (ReplicaSet _ vMembers _) = withMVar vMembers $ mapM_ (maybe (return ()) close . snd)

primary :: ReplicaSet -> IO Pipe
-- ^ Return connection to current primary of replica set. Fail if no primary available.
primary rs@(ReplicaSet rsName _ _) = do
    mHost <- statedPrimary <$> updateMembers rs
    case mHost of
        Just host' -> connection rs Nothing host'
        Nothing -> throwError $ userError $ "replica set " ++ T.unpack rsName ++ " has no primary"

secondaryOk :: ReplicaSet -> IO Pipe
-- ^ Return connection to a random secondary, or primary if no secondaries available.
secondaryOk rs = do
    info <- updateMembers rs
    hosts <- shuffle (possibleHosts info)
    let hosts' = maybe hosts (\p -> delete p hosts ++ [p]) (statedPrimary info)
    untilSuccess (connection rs Nothing) hosts'

routedHost :: ((Host, Bool) -> (Host, Bool) -> IO Ordering) -> ReplicaSet -> IO Pipe
-- ^ Return a connection to a host using a user-supplied sorting function, which sorts based on a tuple containing the host and a boolean indicating whether the host is primary.
routedHost f rs = do
  info <- updateMembers rs
  hosts <- shuffle (possibleHosts info)
  let addIsPrimary h = (h, if Just h == statedPrimary info then True else False)
  hosts' <- mergesortM (\a b -> f (addIsPrimary a) (addIsPrimary b)) hosts
  untilSuccess (connection rs Nothing) hosts'

type ReplicaInfo = (Host, Document)
-- ^ Result of isMaster command on host in replica set. Returned fields are: setName, ismaster, secondary, hosts, [primary]. primary only present when ismaster = false

statedPrimary :: ReplicaInfo -> Maybe Host
-- ^ Primary of replica set or Nothing if there isn't one
statedPrimary (host', info) = if (at "ismaster" info) then Just host' else readHostPort <$> B.lookup "primary" info

possibleHosts :: ReplicaInfo -> [Host]
-- ^ Non-arbiter, non-hidden members of replica set
possibleHosts (_, info) = map readHostPort $ at "hosts" info

updateMembers :: ReplicaSet -> IO ReplicaInfo
-- ^ Fetch replica info from any server and update members accordingly
updateMembers rs@(ReplicaSet _ vMembers _) = do
    (host', info) <- untilSuccess (fetchReplicaInfo rs) =<< readMVar vMembers
    modifyMVar vMembers $ \members -> do
        let ((members', old), new) = intersection (map readHostPort $ at "hosts" info) members
        forM_ old $ \(_, mPipe) -> maybe (return ()) close mPipe
        return (members' ++ map (, Nothing) new, (host', info))
 where
    intersection :: (Eq k) => [k] -> [(k, v)] -> (([(k, v)], [(k, v)]), [k])
    intersection keys assocs = (partition (flip elem inKeys . fst) assocs, keys \\ inKeys) where
        assocKeys = map fst assocs
        inKeys = intersect keys assocKeys

fetchReplicaInfo :: ReplicaSet -> (Host, Maybe Pipe) -> IO ReplicaInfo
-- Connect to host and fetch replica info from host creating new connection if missing or closed (previously failed). Fail if not member of named replica set.
fetchReplicaInfo rs@(ReplicaSet rsName _ _) (host', mPipe) = do
    pipe <- connection rs mPipe host'
    info <- adminCommand ["isMaster" =: (1 :: Int)] pipe
    case B.lookup "setName" info of
        Nothing -> throwError $ userError $ show host' ++ " not a member of any replica set, including " ++ T.unpack rsName ++ ": " ++ show info
        Just setName | setName /= rsName -> throwError $ userError $ show host' ++ " not a member of replica set " ++ T.unpack rsName ++ ": " ++ show info
        Just _ -> return (host', info)

connection :: ReplicaSet -> Maybe Pipe -> Host -> IO Pipe
-- ^ Return new or existing connection to member of replica set. If pipe is already known for host it is given, but we still test if it is open.
connection (ReplicaSet _ vMembers timeoutSecs) mPipe host' =
    maybe conn (\p -> isClosed p >>= \bad -> if bad then conn else return p) mPipe
 where
    conn =  modifyMVar vMembers $ \members -> do
        let new = connect' timeoutSecs host' >>= \pipe -> return (updateAssocs host' (Just pipe) members, pipe)
        case List.lookup host' members of
            Just (Just pipe) -> isClosed pipe >>= \bad -> if bad then new else return (members, pipe)
            _ -> new


{- Authors: Tony Hannan <tony@10gen.com>
   Copyright 2011 10gen Inc.
   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. -}
