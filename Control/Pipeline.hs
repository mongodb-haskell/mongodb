{- | Pipelining is sending multiple requests over a socket and receiving the responses later, in the same order. This is faster than sending one request, waiting for the response, then sending the next request, and so on. This implementation returns a /promise (future)/ response for each request that when invoked waits for the response if not already arrived. Multiple threads can send on the same pipe (and get promises back); the pipe will pipeline each thread's request right away without waiting. -}

{-# LANGUAGE DoRec, RecordWildCards, MultiParamTypeClasses, FlexibleContexts #-}

module Control.Pipeline (
	-- * Pipe
	Pipe, newPipe, send, call,
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
import Control.Exception (assert)
import System.IO.Error (try)
import System.IO (Handle, hFlush, hClose, hIsClosed)
import qualified Data.ByteString as S
import qualified Data.ByteString.Lazy as L
import Data.Monoid (Monoid(..))
import Control.Concurrent (ThreadId, forkIO, killThread)
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
	isClosed :: r -> m Bool

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
	-- ^ Read up to N bytes from handle, block until at least 1 byte is available

getN :: (Stream h b) => h -> Int -> IO b
-- ^ Read N bytes from hande, blocking until all N bytes are read. Unlike 'get' which only blocks if no bytes are available.
getN h n = assert (n >= 0) $ do
	bytes <- get h n
	let x = length bytes
	if x >= n then return bytes else do
		remainingBytes <- getN h (n - x)
		return (mappend bytes remainingBytes)

instance Stream Handle S.ByteString where
	put = S.hPut
	get = S.hGet

instance Stream Handle L.ByteString where
	put = L.hPut
	get = L.hGet

-- * Pipe

-- | Thread-safe and pipelined socket
data Pipe handle bytes = Pipe {
	encodeSize :: Size -> bytes,
	decodeSize :: bytes -> Size,
	vHandle :: MVar handle,  -- ^ Mutex on handle, so only one thread at a time can write to it
	responseQueue :: Chan (MVar (Either IOError bytes)),  -- ^ Queue of threads waiting for responses. Every time a response arrive we pop the next thread and give it the response.
	listenThread :: ThreadId
	}

-- | Create new Pipe with given encodeInt, decodeInt, and handle. You should 'close' pipe when finished, which will also close handle. If pipe is not closed but eventually garbage collected, it will be closed along with handle.
newPipe :: (Stream h b, Resource IO h) =>
	(Size -> b)  -- ^ Convert Size to bytes of fixed length. Every Int must translate to same number of bytes.
	-> (b -> Size)  -- ^ Convert bytes of fixed length to Size. Must be exact inverse of encodeSize.
	-> h  -- ^ Underlying socket (handle) this pipe will read/write from
	-> IO (Pipe h b)
newPipe encodeSize decodeSize handle = do
	vHandle <- newMVar handle
	responseQueue <- newChan
	rec
		let pipe = Pipe{..}
		listenThread <- forkIO (listen pipe)
	addMVarFinalizer vHandle $ do
		killThread listenThread
		close handle
	return pipe

instance (Resource IO h) => Resource IO (Pipe h b) where
	-- | Close pipe and underlying socket (handle)
	close Pipe{..} = do
		killThread listenThread
		close =<< readMVar vHandle
	isClosed Pipe{..} = isClosed =<< readMVar vHandle

listen :: (Stream h b) => Pipe h b -> IO ()
-- ^ Listen for responses and supply them to waiting threads in order
listen Pipe{..} = do
	let n = length (encodeSize 0)
	h <- readMVar vHandle
	forever $ do
		e <- try $ do
			len <- decodeSize <$> getN h n
			getN h len
		var <- readChan responseQueue
		putMVar var e

send :: (Stream h b) => Pipe h b -> [b] -> IO ()
-- ^ Send messages all together to destination (no messages will be interleaved between them). None of the messages can induce a response, i.e. the destination must not reply to any of these messages (otherwise future 'call's will get these responses instead of their own).
-- Each message is preceeded by its length when written to socket.
send Pipe{..} messages = withMVar vHandle $ \h -> do
	mapM_ (write encodeSize h) messages
	flush h

call :: (Stream h b) => Pipe h b -> [b] -> IO (IO b)
-- ^ Send messages all together to destination (no messages will be interleaved between them), and return /promise/ of response from one message only. One and only one message in the list must induce a response, i.e. the destination must reply to exactly one message only (otherwise promises will have the wrong responses in them).
-- Each message is preceeded by its length when written to socket. Likewise, the response must be preceeded by its length.
call Pipe{..} messages = withMVar vHandle $ \h -> do
	mapM_ (write encodeSize h) messages
	flush h
	var <- newEmptyMVar
	writeChan responseQueue var
	return (either ioError return =<< readMVar var)  -- return promise

write :: (Stream h b, Monoid b, Length b) => (Size -> b) -> h -> b -> IO ()
write encodeSize h bytes = put h (mappend lenBytes bytes) where lenBytes = encodeSize (length bytes)
