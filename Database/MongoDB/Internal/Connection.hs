{-| Low-level connection to a server.

This module is not intended for direct use. Use the high-level interface at "Database.MongoDB.Connection" instead. -}

{-# LANGUAGE GeneralizedNewtypeDeriving, TupleSections, TypeSynonymInstances, OverlappingInstances #-}

module Database.MongoDB.Internal.Connection (
	-- * Server
	Server(..),
	-- * Connection
	Connection, connServer, showHandle, mkConnection, withConn, closeConnection, isClosed,
	-- * Connected monad
	Conn(..), Failure(..),
	-- ** Task
	Task, runTask,
	-- ** Op
	Op, sendBytes, flushBytes, receiveBytes,
	exposeIO, hideIO
) where

import Control.Applicative (Applicative(..), (<$>))
import Control.Arrow (left)
import System.IO.Error (try)
import Control.Concurrent.MVar
import Control.Monad.Reader
import Control.Monad.Error
import Network (HostName, PortID(..))
import System.IO (Handle, hFlush, hClose, hIsClosed)
import Data.ByteString.Lazy as B (ByteString, hPut)
import System.Timeout
import Database.MongoDB.Util (Secs, ignore, hGetN)  -- Reader/Error Applicative instances

-- * Server

data Server = Server HostName PortID  deriving (Show, Eq, Ord)

-- * Connection

data Connection = Connection Server (MVar Handle)  deriving (Eq)
-- ^ A connection to a server. This connection holds session state on the server like open cursors and temporary map-reduce tables that disappear when the connection is closed or fails.

connServer :: Connection -> Server
-- ^ Server this connection is connected to
connServer (Connection serv _) = serv

showHandle :: Secs -> Connection -> IO String
-- ^ Show handle if not locked for more than given seconds
showHandle secs (Connection _ vHand) =
	maybe "handle currently locked" show <$> timeout (round (secs * 1000000)) (readMVar vHand)

instance Show Connection where
	showsPrec d c = showParen (d > 10) $ showString "a connection to " . showsPrec 11 (connServer c)

mkConnection :: Server -> Handle -> IO Connection
-- ^ Wrap handle in a MVar to control access
mkConnection s h = Connection s <$> newMVar h

withConn :: Connection -> (Handle -> IO a) -> IO a
-- Execute IO action with exclusive access to TCP connection
withConn (Connection _ vHand) = withMVar vHand

closeConnection :: Connection -> IO ()
-- ^ Close connection. Attempting to read or write to a closed connection will raise 'Failure' exception.
closeConnection (Connection _ vHand) = withMVar vHand $ \h -> catch (hClose h) ignore

isClosed :: Connection -> IO Bool
-- ^ Is connection closed?
isClosed (Connection _ vHand) = withMVar vHand hIsClosed

-- * Task

-- | Connection or Server failure like network problem or disk full
data Failure =
	ConnectionFailure IOError
	-- ^ Error during sending or receiving bytes over a 'Connection'. The connection is not automatically closed when this error happens; the user must close it. Any other IOErrors raised during a Task or Op are not caught. The user is responsible for these other types of errors not related to sending/receiving bytes over the connection.
	| ServerFailure String
	-- ^ Failure on server, like disk full, which is usually observed using getLastError. Calling 'fail' inside a Task or Op raises this failure. Do not call 'fail' unless it is a temporary server failure, like disk full. For example, receiving unexpected data from the server is not a server failure, rather it is a programming error (you should call 'error' in this case) because the client and server are incompatible and requires a programming change.
	deriving (Show, Eq)

instance Error Failure where strMsg = ServerFailure

type Task m = ErrorT Failure (ReaderT Connection m)
-- ^ Action with shared access to connection (the connection can be supplied to multiple concurrent tasks). m must be a MonadIO.

runTask :: Task m a -> Connection -> m (Either Failure a)
-- ^ Run task with shared access to connection. Return Left if connection fails anytime during its execution, in which case the task was partially executed.
runTask = runReaderT . runErrorT

-- * Op

newtype Op a = Op (ErrorT Failure (ReaderT (Connection, Handle) IO) a)
	deriving (Functor, Applicative, Monad, MonadIO, MonadError Failure)
-- ^ Action with exclusive access to connection (other ops must wait)

runOp' :: (MonadIO m) => Op a -> Task m a
-- ^ Run operation with exclusive access to connection. Fail if connection fails anytime during its execution, in which case the operation was partially executed.
runOp' (Op act) = ErrorT . ReaderT $ \conn ->
	liftIO . withConn conn $ runReaderT (runErrorT act) . (conn,)

sendBytes :: ByteString -> Op ()
-- ^ Put bytes on socket
sendBytes bytes = Op . ErrorT . ReaderT $ \(_, h) -> left ConnectionFailure <$> try (hPut h bytes)

flushBytes :: Op ()
-- ^ Flush socket
flushBytes = Op . ErrorT . ReaderT $ \(_, h) -> left ConnectionFailure <$> try (hFlush h)

receiveBytes :: Int -> Op ByteString
-- ^ Get N bytes from socket, blocking until all N bytes are received
receiveBytes n = Op . ErrorT . ReaderT $ \(_, h) -> left ConnectionFailure <$> try (hGetN h n)

exposeIO :: ((Connection, Handle) -> IO (Either Failure a)) -> Op a
-- ^ Expose connection to underlying IO
exposeIO = Op . ErrorT . ReaderT

hideIO :: Op a -> (Connection, Handle) -> IO (Either Failure a)
-- ^ Run op from IO
hideIO (Op act) = runReaderT (runErrorT act)

-- * Connected monad

-- | A monad with shared or exclusive access to a connection, ie. 'Task' or 'Op'
class (Functor m, Applicative m, MonadIO m) => Conn m where
	runOp :: Op a -> m a
	-- ^ Run op with exclusive access to connection. If @m@ is already exclusive then simply run op.
	getConnection :: m Connection
	-- ^ Return connection that this monad has access to

instance (MonadIO m) => Conn (Task m) where
	runOp = runOp'
	getConnection = ask

instance Conn Op where
	runOp = id
	getConnection = Op (asks fst)

instance (Conn m) => Conn (ReaderT r m) where
	runOp = lift . runOp
	getConnection = lift getConnection

instance (Conn m, Error e) => Conn (ErrorT e m) where
	runOp = lift . runOp
	getConnection = lift getConnection


{- Authors: Tony Hannan <tony@10gen.com>
   Copyright 2010 10gen Inc.
   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. -}
