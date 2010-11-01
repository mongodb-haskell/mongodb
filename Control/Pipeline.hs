{- | Pipelining is sending multiple requests over a socket and receiving the responses later, in the same order. This is faster than sending one request, waiting for the response, then sending the next request, and so on. This implementation returns a /promise (future)/ response for each request that when invoked waits for the response if not already arrived. Multiple threads can send on the same pipeline (and get promises back); it will pipeline each thread's request right away without waiting.

A pipeline closes itself when a read or write causes an error, so you can detect a broken pipeline by checking isClosed.  It also closes itself when garbage collected, or you can close it explicitly. -}

{-# LANGUAGE DoRec, RecordWildCards, NamedFieldPuns, MultiParamTypeClasses, FlexibleContexts #-}

module Control.Pipeline (
	-- * Pipeline
	Pipeline, newPipeline, send, call,
	-- * Util
	Size,
	Length(..),
	Resource(..),
	Flush(..),
	Stream(..), getN
) where

import Prelude hiding (length)
import Control.Applicative ((<$>))
import Control.Monad (forever)
import Control.Exception (assert, onException)
import System.IO.Error (try, mkIOError, eofErrorType)
import System.IO (Handle, hFlush, hClose, hIsClosed)
import qualified Data.ByteString as S
import qualified Data.ByteString.Lazy as L
import Data.Monoid (Monoid(..))
import Control.Concurrent (ThreadId, forkIO, killThread)
import GHC.Conc (ThreadStatus(..), threadStatus)
import Control.Concurrent.MVar
import Control.Concurrent.Chan

-- * Length

type Size = Int

class Length list where
	length :: list -> Size

instance Length S.ByteString where
	length = S.length

instance Length L.ByteString where
	length = fromEnum . L.length

-- * Resource

class Resource m r where
	close :: r -> m ()
	-- ^ Close resource
	isClosed :: r -> m Bool
	-- ^ Is resource closed

instance Resource IO Handle where
	close = hClose
	isClosed = hIsClosed

-- * Flush

class Flush handle where
	flush :: handle -> IO ()
	-- ^ Flush written bytes to destination

instance Flush Handle where
	flush = hFlush

-- * Stream

class (Length bytes, Monoid bytes, Flush handle) => Stream handle bytes where
	put :: handle -> bytes -> IO ()
	-- ^ Write bytes to handle
	get :: handle -> Int -> IO bytes
	-- ^ Read up to N bytes from handle; if EOF return empty bytes, otherwise block until at least 1 byte is available

getN :: (Stream h b) => h -> Int -> IO b
-- ^ Read N bytes from hande, blocking until all N bytes are read. If EOF is reached before N bytes then throw EOF exception.
getN h n = assert (n >= 0) $ do
	bytes <- get h n
	let x = length bytes
	if x >= n then return bytes
		else if x == 0 then ioError (mkIOError eofErrorType "Control.Pipeline" Nothing Nothing)
			else mappend bytes <$> getN h (n - x)

instance Stream Handle S.ByteString where
	put = S.hPut
	get = S.hGet

instance Stream Handle L.ByteString where
	put = L.hPut
	get = L.hGet

-- * Pipeline

-- | Thread-safe and pipelined socket
data Pipeline handle bytes = Pipeline {
	encodeSize :: Size -> bytes,
	decodeSize :: bytes -> Size,
	vHandle :: MVar handle,  -- ^ Mutex on handle, so only one thread at a time can write to it
	responseQueue :: Chan (MVar (Either IOError bytes)),  -- ^ Queue of threads waiting for responses. Every time a response arrive we pop the next thread and give it the response.
	listenThread :: ThreadId
	}

-- | Create new Pipeline with given encodeInt, decodeInt, and handle. You should 'close' pipeline when finished, which will also close handle. If pipeline is not closed but eventually garbage collected, it will be closed along with handle.
newPipeline :: (Stream h b, Resource IO h) =>
	(Size -> b)  -- ^ Convert Size to bytes of fixed length. Every Int must translate to same number of bytes.
	-> (b -> Size)  -- ^ Convert bytes of fixed length to Size. Must be exact inverse of encodeSize.
	-> h  -- ^ Underlying socket (handle) this pipeline will read/write from
	-> IO (Pipeline h b)
newPipeline encodeSize decodeSize handle = do
	vHandle <- newMVar handle
	responseQueue <- newChan
	rec
		let pipe = Pipeline{..}
		listenThread <- forkIO (listen pipe)
	addMVarFinalizer vHandle $ do
		killThread listenThread
		close handle
	return pipe

instance (Resource IO h) => Resource IO (Pipeline h b) where
	-- | Close pipe and underlying socket (handle)
	close Pipeline{..} = do
		killThread listenThread
		close =<< readMVar vHandle
	isClosed Pipeline{listenThread} = do
		status <- threadStatus listenThread
		return $ case status of
			ThreadRunning -> False
			ThreadFinished -> True
			ThreadBlocked _ -> False
			ThreadDied -> True
	--isClosed Pipeline{..} = isClosed =<< readMVar vHandle  -- isClosed hangs while listen loop is waiting on read

listen :: (Stream h b, Resource IO h) => Pipeline h b -> IO ()
-- ^ Listen for responses and supply them to waiting threads in order
listen Pipeline{..} = do
	let n = length (encodeSize 0)
	h <- readMVar vHandle
	forever $ do
		e <- try $ do
			len <- decodeSize <$> getN h n
			getN h len
		var <- readChan responseQueue
		putMVar var e
		case e of
			Left err -> close h >> fail (show err)  -- close and stop looping
			Right _ -> return ()

send :: (Stream h b, Resource IO h) => Pipeline h b -> [b] -> IO ()
-- ^ Send messages all together to destination (no messages will be interleaved between them). None of the messages can induce a response, i.e. the destination must not reply to any of these messages (otherwise future 'call's will get these responses instead of their own).
-- Each message is preceeded by its length when written to socket.
-- Raises IOError and closes pipeline if send fails
send Pipeline{..} messages = withMVar vHandle (writeAll listenThread encodeSize messages)

call :: (Stream h b, Resource IO h) => Pipeline h b -> [b] -> IO (IO b)
-- ^ Send messages all together to destination (no messages will be interleaved between them), and return /promise/ of response from one message only. One and only one message in the list must induce a response, i.e. the destination must reply to exactly one message only (otherwise promises will have the wrong responses in them).
-- Each message is preceeded by its length when written to socket. Likewise, the response must be preceeded by its length.
-- Raises IOError and closes pipeline if send fails, likewise for reply.
call Pipeline{..} messages = withMVar vHandle $ \h -> do
	writeAll listenThread encodeSize messages h
	var <- newEmptyMVar
	writeChan responseQueue var
	return (either ioError return =<< readMVar var)  -- return promise

writeAll :: (Stream h b, Monoid b, Length b, Resource IO h) => ThreadId -> (Size -> b) -> [b] -> h -> IO ()
-- ^ Write messages to stream. On error, close pipeline and raise IOError.
writeAll listenThread encodeSize messages h = onException
	(mapM_ write messages >> flush h)
	(killThread listenThread >> close h)
  where
	write bytes = put h (mappend lenBytes bytes) where lenBytes = encodeSize (length bytes)
