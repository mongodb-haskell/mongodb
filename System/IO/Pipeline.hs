{- | Pipelining is sending multiple requests over a socket and receiving the responses later in the same order (a' la HTTP pipelining). This is faster than sending one request, waiting for the response, then sending the next request, and so on. This implementation returns a /promise (future)/ response for each request that when invoked waits for the response if not already arrived. Multiple threads can send on the same pipeline (and get promises back); it will send each thread's request right away without waiting.

A pipeline closes itself when a read or write causes an error, so you can detect a broken pipeline by checking isClosed.  It also closes itself when garbage collected, or you can close it explicitly. -}

{-# LANGUAGE RecordWildCards, NamedFieldPuns, ScopedTypeVariables #-}
{-# LANGUAGE CPP #-}

#if (__GLASGOW_HASKELL__ >= 706)
{-# LANGUAGE RecursiveDo #-}
#else
{-# LANGUAGE DoRec #-}
#endif

module System.IO.Pipeline (
    -- * IOStream
    IOStream(..),
    -- * Pipeline
    Pipeline, newPipeline, send, call, close, isClosed
) where

import Prelude hiding (length)
import Control.Concurrent (ThreadId, forkIO, killThread)
import Control.Concurrent.Chan (Chan, newChan, readChan, writeChan)
import Control.Monad (forever)
import GHC.Conc (ThreadStatus(..), threadStatus)

import Control.Monad.Trans (liftIO)
#if MIN_VERSION_base(4,6,0)
import Control.Concurrent.MVar.Lifted (MVar, newEmptyMVar, newMVar, withMVar,
                                       putMVar, readMVar, mkWeakMVar)
#else
import Control.Concurrent.MVar.Lifted (MVar, newEmptyMVar, newMVar, withMVar,
                                         putMVar, readMVar, addMVarFinalizer)
#endif
import Control.Exception.Lifted (onException, throwIO, try)

#if !MIN_VERSION_base(4,6,0)
mkWeakMVar :: MVar a -> IO () -> IO ()
mkWeakMVar = addMVarFinalizer
#endif

-- * IOStream

-- | An IO sink and source where value of type @o@ are sent and values of type @i@ are received.
data IOStream i o = IOStream {
    writeStream :: o -> IO (),
    readStream :: IO i,
    closeStream :: IO () }

-- * Pipeline

-- | Thread-safe and pipelined connection
data Pipeline i o = Pipeline {
    vStream :: MVar (IOStream i o),  -- ^ Mutex on handle, so only one thread at a time can write to it
    responseQueue :: Chan (MVar (Either IOError i)),  -- ^ Queue of threads waiting for responses. Every time a response arrive we pop the next thread and give it the response.
    listenThread :: ThreadId
    }

-- | Create new Pipeline over given handle. You should 'close' pipeline when finished, which will also close handle. If pipeline is not closed but eventually garbage collected, it will be closed along with handle.
newPipeline :: IOStream i o -> IO (Pipeline i o)
newPipeline stream = do
    vStream <- newMVar stream
    responseQueue <- newChan
    rec
        let pipe = Pipeline{..}
        listenThread <- forkIO (listen pipe)
    _ <- mkWeakMVar vStream $ do
        killThread listenThread
        closeStream stream
    return pipe

close :: Pipeline i o -> IO ()
-- ^ Close pipe and underlying connection
close Pipeline{..} = do
    killThread listenThread
    closeStream =<< readMVar vStream

isClosed :: Pipeline i o -> IO Bool
isClosed Pipeline{listenThread} = do
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
    stream <- readMVar vStream
    forever $ do
        e <- try $ readStream stream
        var <- readChan responseQueue
        putMVar var e
        case e of
            Left err -> closeStream stream >> ioError err  -- close and stop looping
            Right _ -> return ()

send :: Pipeline i o -> o -> IO ()
-- ^ Send message to destination; the destination must not response (otherwise future 'call's will get these responses instead of their own).
-- Throw IOError and close pipeline if send fails
send p@Pipeline{..} message = withMVar vStream (flip writeStream message) `onException` close p

call :: Pipeline i o -> o -> IO (IO i)
-- ^ Send message to destination and return /promise/ of response from one message only. The destination must reply to the message (otherwise promises will have the wrong responses in them).
-- Throw IOError and closes pipeline if send fails, likewise for promised response.
call p@Pipeline{..} message = withMVar vStream doCall `onException` close p  where
    doCall stream = do
        writeStream stream message
        var <- newEmptyMVar
        liftIO $ writeChan responseQueue var
        return $ readMVar var >>= either throwIO return -- return promise


{- Authors: Tony Hannan <tony@10gen.com>
   Copyright 2011 10gen Inc.
   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. -}
