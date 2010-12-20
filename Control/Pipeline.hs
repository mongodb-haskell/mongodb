{- | Pipelining is sending multiple requests over a socket and receiving the responses later, in the same order. This is faster than sending one request, waiting for the response, then sending the next request, and so on. This implementation returns a /promise (future)/ response for each request that when invoked waits for the response if not already arrived. Multiple threads can send on the same pipeline (and get promises back); it will pipeline each thread's request right away without waiting.

A pipeline closes itself when a read or write causes an error, so you can detect a broken pipeline by checking isClosed.  It also closes itself when garbage collected, or you can close it explicitly. -}

{-# LANGUAGE DoRec, RecordWildCards, NamedFieldPuns, ScopedTypeVariables #-}

module Control.Pipeline (
	-- * Pipeline
	Pipeline, newPipeline, send, call, close, isClosed
) where

import Control.Monad.Throw (onException)
import Control.Monad.Error
import Control.Concurrent (ThreadId, forkIO, killThread)
import GHC.Conc (ThreadStatus(..), threadStatus)
import Control.Monad.MVar
import Control.Concurrent.Chan
import Network.Abstract (IOE)
import qualified Network.Abstract as C

-- * Pipeline

-- | Thread-safe and pipelined connection
data Pipeline i o = Pipeline {
	vConn :: MVar (C.Connection i o),  -- ^ Mutex on handle, so only one thread at a time can write to it
	responseQueue :: Chan (MVar (Either IOError o)),  -- ^ Queue of threads waiting for responses. Every time a response arrive we pop the next thread and give it the response.
	listenThread :: ThreadId
	}

-- | Create new Pipeline on given connection. You should 'close' pipeline when finished, which will also close connection. If pipeline is not closed but eventually garbage collected, it will be closed along with connection.
newPipeline :: (MonadIO m) => C.Connection i o -> m (Pipeline i o)
newPipeline conn = liftIO $ do
	vConn <- newMVar conn
	responseQueue <- newChan
	rec
		let pipe = Pipeline{..}
		listenThread <- forkIO (listen pipe)
	addMVarFinalizer vConn $ do
		killThread listenThread
		C.close conn
	return pipe

close :: (MonadIO m) => Pipeline i o -> m ()
-- | Close pipe and underlying connection
close Pipeline{..} = liftIO $ do
	killThread listenThread
	C.close =<< readMVar vConn

isClosed :: (MonadIO m) => Pipeline i o -> m Bool
isClosed Pipeline{listenThread} = liftIO $ do
	status <- threadStatus listenThread
	return $ case status of
		ThreadRunning -> False
		ThreadFinished -> True
		ThreadBlocked _ -> False
		ThreadDied -> True
--isPipeClosed Pipeline{..} = isClosed =<< readMVar vHandle  -- isClosed hangs while listen loop is waiting on read

listen :: Pipeline i o -> IO ()
-- ^ Listen for responses and supply them to waiting threads in order
listen Pipeline{..} = do
	conn <- readMVar vConn
	forever $ do
		e <- runErrorT $ C.receive conn
		var <- readChan responseQueue
		putMVar var e
		case e of
			Left err -> C.close conn >> ioError err  -- close and stop looping
			Right _ -> return ()

send :: Pipeline i o -> i -> IOE ()
-- ^ Send message to destination; the destination must not response (otherwise future 'call's will get these responses instead of their own).
-- Throw IOError and close pipeline if send fails
send p@Pipeline{..} message = withMVar vConn (flip C.send message) `onException` \(_ :: IOError) -> close p

call :: Pipeline i o -> i -> IOE (IOE o)
-- ^ Send message to destination and return /promise/ of response from one message only. The destination must reply to the message (otherwise promises will have the wrong responses in them).
-- Throw IOError and closes pipeline if send fails, likewise for promised response.
call p@Pipeline{..} message = withMVar vConn doCall `onException` \(_ :: IOError) -> close p  where
	doCall conn = do
		C.send conn message
		var <- newEmptyMVar
		liftIO $ writeChan responseQueue var
		return $ ErrorT (readMVar var)  -- return promise


{- Authors: Tony Hannan <tony@10gen.com>
   Copyright 2010 10gen Inc.
   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. -}
