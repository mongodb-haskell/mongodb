-- | Low-level messaging between this client and the MongoDB server, see Mongo
-- Wire Protocol (<http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol>).
--
-- This module is not intended for direct use. Use the high-level interface at
-- "Database.MongoDB.Query" and "Database.MongoDB.Connection" instead.

{-# LANGUAGE RecordWildCards, StandaloneDeriving, OverloadedStrings #-}
{-# LANGUAGE CPP, FlexibleContexts, TupleSections, TypeSynonymInstances #-}
{-# LANGUAGE MultiParamTypeClasses, FlexibleInstances, UndecidableInstances #-}

{-# LANGUAGE NamedFieldPuns, ScopedTypeVariables #-}

#if (__GLASGOW_HASKELL__ >= 706)
{-# LANGUAGE RecursiveDo #-}
#else
{-# LANGUAGE DoRec #-}
#endif

module Database.MongoDB.Internal.Protocol (
    FullCollection,
    -- * Pipe
    Pipe, newPipe, send, call,
    -- ** Notice
    Notice(..), InsertOption(..), UpdateOption(..), DeleteOption(..), CursorId,
    -- ** Request
    Request(..), QueryOption(..),
    -- ** Reply
    Reply(..), ResponseFlag(..),
    -- * Authentication
    Username, Password, Nonce, pwHash, pwKey,
    isClosed, close
) where

#if !MIN_VERSION_base(4,8,0)
import Control.Applicative ((<$>))
#endif
import Control.Arrow ((***))
import Control.Monad (forM, replicateM, unless)
import Data.Binary.Get (Get, runGet)
import Data.Binary.Put (Put, runPut)
import Data.Bits (bit, testBit)
import Data.Int (Int32, Int64)
import Data.IORef (IORef, newIORef, atomicModifyIORef)
import System.IO (Handle)
import System.IO.Unsafe (unsafePerformIO)
import Data.Maybe (maybeToList)
import GHC.Conc (ThreadStatus(..), threadStatus)
import Control.Monad (forever)
import Control.Concurrent.Chan (Chan, newChan, readChan, writeChan)
import Control.Concurrent (ThreadId, forkIO, killThread)

import Control.Exception.Lifted (onException, throwIO, try)
import qualified Control.Exception.Lifted as CEL

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L

import Control.Monad.Trans (MonadIO, liftIO)
import Data.Bson (Document)
import Data.Bson.Binary (getDocument, putDocument, getInt32, putInt32, getInt64,
                         putInt64, putCString)
import Data.Text (Text)

import qualified Crypto.Hash.MD5 as MD5
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE

import Database.MongoDB.Internal.Util (whenJust, bitOr, byteStringHex)
import System.IO (hClose, hFlush)

#if MIN_VERSION_base(4,6,0)
import Control.Concurrent.MVar.Lifted (MVar, newEmptyMVar, newMVar, withMVar,
                                       putMVar, readMVar, mkWeakMVar)
#else
import Control.Concurrent.MVar.Lifted (MVar, newEmptyMVar, newMVar, withMVar,
                                         putMVar, readMVar, addMVarFinalizer)
#endif

#if !MIN_VERSION_base(4,6,0)
mkWeakMVar :: MVar a -> IO () -> IO ()
mkWeakMVar = addMVarFinalizer
#endif

-- * Pipeline

-- | Thread-safe and pipelined connection
data Pipeline = Pipeline {
    vStream :: MVar Handle,  -- ^ Mutex on handle, so only one thread at a time can write to it
    responseQueue :: Chan (MVar (Either IOError Response)),  -- ^ Queue of threads waiting for responses. Every time a response arrive we pop the next thread and give it the response.
    listenThread :: ThreadId
    }

-- | Create new Pipeline over given handle. You should 'close' pipeline when finished, which will also close handle. If pipeline is not closed but eventually garbage collected, it will be closed along with handle.
newPipeline :: Handle -> IO Pipeline
newPipeline stream = do
    vStream <- newMVar stream
    responseQueue <- newChan
    rec
        let pipe = Pipeline{..}
        listenThread <- forkIO (listen pipe)
    _ <- mkWeakMVar vStream $ do
        killThread listenThread
        hClose stream
    return pipe

close :: Pipeline -> IO ()
-- ^ Close pipe and underlying connection
close Pipeline{..} = do
    killThread listenThread
    hClose =<< readMVar vStream

isClosed :: Pipeline -> IO Bool
isClosed Pipeline{listenThread} = do
    status <- threadStatus listenThread
    return $ case status of
        ThreadRunning -> False
        ThreadFinished -> True
        ThreadBlocked _ -> False
        ThreadDied -> True
--isPipeClosed Pipeline{..} = isClosed =<< readMVar vHandle  -- isClosed hangs while listen loop is waiting on read

listen :: Pipeline -> IO ()
-- ^ Listen for responses and supply them to waiting threads in order
listen Pipeline{..} = do
    stream <- readMVar vStream
    forever $ do
        e <- try $ readMessage stream
        var <- readChan responseQueue
        putMVar var e
        case e of
            Left err -> hClose stream >> ioError err  -- close and stop looping
            Right _ -> return ()

psend :: Pipeline -> Message -> IO ()
-- ^ Send message to destination; the destination must not response (otherwise future 'call's will get these responses instead of their own).
-- Throw IOError and close pipeline if send fails
psend p@Pipeline{..} message = withMVar vStream (flip writeMessage message) `onException` close p

pcall :: Pipeline -> Message -> IO (IO Response)
-- ^ Send message to destination and return /promise/ of response from one message only. The destination must reply to the message (otherwise promises will have the wrong responses in them).
-- Throw IOError and closes pipeline if send fails, likewise for promised response.
pcall p@Pipeline{..} message = withMVar vStream doCall `onException` close p  where
    doCall stream = do
        writeMessage stream message
        var <- newEmptyMVar
        liftIO $ writeChan responseQueue var
        return $ readMVar var >>= either throwIO return -- return promise

-- * Pipe

type Pipe = Pipeline
-- ^ Thread-safe TCP connection with pipelined requests

newPipe :: Handle -> IO Pipe
-- ^ Create pipe over handle
newPipe handle = newPipeline handle

send :: Pipe -> [Notice] -> IO ()
-- ^ Send notices as a contiguous batch to server with no reply. Throw IOError if connection fails.
send pipe notices = psend pipe (notices, Nothing)

call :: Pipe -> [Notice] -> Request -> IO (IO Reply)
-- ^ Send notices and request as a contiguous batch to server and return reply promise, which will block when invoked until reply arrives. This call and resulting promise will throw IOError if connection fails.
call pipe notices request = do
    requestId <- genRequestId
    promise <- pcall pipe (notices, Just (request, requestId))
    return $ check requestId <$> promise
 where
    check requestId (responseTo, reply) = if requestId == responseTo then reply else
        error $ "expected response id (" ++ show responseTo ++ ") to match request id (" ++ show requestId ++ ")"

-- * Message

type Message = ([Notice], Maybe (Request, RequestId))
-- ^ A write notice(s) with getLastError request, or just query request.
-- Note, that requestId will be out of order because request ids will be generated for notices after the request id supplied was generated. This is ok because the mongo server does not care about order just uniqueness.

writeMessage :: Handle -> Message -> IO ()
-- ^ Write message to connection
writeMessage h (notices, mRequest) = do
    noticeStrings <- forM notices $ \n -> do
          requestId <- genRequestId
          let s = runPut $ putNotice n requestId
          return $ (lenBytes s) `L.append` s

    let requestString = do
          (request, requestId) <- mRequest
          let s = runPut $ putRequest request requestId
          return $ (lenBytes s) `L.append` s

    B.hPut h $ L.toStrict $ L.concat $ noticeStrings ++ (maybeToList requestString)
    hFlush h
 where
    lenBytes bytes = encodeSize . toEnum . fromEnum $ L.length bytes
    encodeSize = runPut . putInt32 . (+ 4)

type Response = (ResponseTo, Reply)
-- ^ Message received from a Mongo server in response to a Request

readMessage :: Handle -> IO Response
-- ^ read response from a connection
readMessage h = readResp  where
    readResp = do
        len <- fromEnum . decodeSize <$> B.hGet h 4
        runGet getReply <$> (L.fromStrict <$> B.hGet h len)
    decodeSize = subtract 4 . runGet getInt32 . L.fromStrict

type FullCollection = Text
-- ^ Database name and collection name with period (.) in between. Eg. \"myDb.myCollection\"

-- ** Header

type Opcode = Int32

type RequestId = Int32
-- ^ A fresh request id is generated for every message

type ResponseTo = RequestId

genRequestId :: (MonadIO m) => m RequestId
-- ^ Generate fresh request id
genRequestId = liftIO $ atomicModifyIORef counter $ \n -> (n + 1, n) where
    counter :: IORef RequestId
    counter = unsafePerformIO (newIORef 0)
    {-# NOINLINE counter #-}

-- *** Binary format

putHeader :: Opcode -> RequestId -> Put
-- ^ Note, does not write message length (first int32), assumes caller will write it
putHeader opcode requestId = do
    putInt32 requestId
    putInt32 0
    putInt32 opcode

getHeader :: Get (Opcode, ResponseTo)
-- ^ Note, does not read message length (first int32), assumes it was already read
getHeader = do
    _requestId <- getInt32
    responseTo <- getInt32
    opcode <- getInt32
    return (opcode, responseTo)

-- ** Notice

-- | A notice is a message that is sent with no reply
data Notice =
      Insert {
        iFullCollection :: FullCollection,
        iOptions :: [InsertOption],
        iDocuments :: [Document]}
    | Update {
        uFullCollection :: FullCollection,
        uOptions :: [UpdateOption],
        uSelector :: Document,
        uUpdater :: Document}
    | Delete {
        dFullCollection :: FullCollection,
        dOptions :: [DeleteOption],
        dSelector :: Document}
    | KillCursors {
        kCursorIds :: [CursorId]}
    deriving (Show, Eq)

data InsertOption = KeepGoing  -- ^ If set, the database will not stop processing a bulk insert if one fails (eg due to duplicate IDs). This makes bulk insert behave similarly to a series of single inserts, except lastError will be set if any insert fails, not just the last one. (new in 1.9.1)
    deriving (Show, Eq)

data UpdateOption =
      Upsert  -- ^ If set, the database will insert the supplied object into the collection if no matching document is found
    | MultiUpdate  -- ^ If set, the database will update all matching objects in the collection. Otherwise only updates first matching doc
    deriving (Show, Eq)

data DeleteOption = SingleRemove  -- ^ If set, the database will remove only the first matching document in the collection. Otherwise all matching documents will be removed
    deriving (Show, Eq)

type CursorId = Int64

-- *** Binary format

nOpcode :: Notice -> Opcode
nOpcode Update{} = 2001
nOpcode Insert{} = 2002
nOpcode Delete{} = 2006
nOpcode KillCursors{} = 2007

putNotice :: Notice -> RequestId -> Put
putNotice notice requestId = do
    putHeader (nOpcode notice) requestId
    case notice of
        Insert{..} -> do
            putInt32 (iBits iOptions)
            putCString iFullCollection
            mapM_ putDocument iDocuments
        Update{..} -> do
            putInt32 0
            putCString uFullCollection
            putInt32 (uBits uOptions)
            putDocument uSelector
            putDocument uUpdater
        Delete{..} -> do
            putInt32 0
            putCString dFullCollection
            putInt32 (dBits dOptions)
            putDocument dSelector
        KillCursors{..} -> do
            putInt32 0
            putInt32 $ toEnum (length kCursorIds)
            mapM_ putInt64 kCursorIds

iBit :: InsertOption -> Int32
iBit KeepGoing = bit 0

iBits :: [InsertOption] -> Int32
iBits = bitOr . map iBit

uBit :: UpdateOption -> Int32
uBit Upsert = bit 0
uBit MultiUpdate = bit 1

uBits :: [UpdateOption] -> Int32
uBits = bitOr . map uBit

dBit :: DeleteOption -> Int32
dBit SingleRemove = bit 0

dBits :: [DeleteOption] -> Int32
dBits = bitOr . map dBit

-- ** Request

-- | A request is a message that is sent with a 'Reply' expected in return
data Request =
      Query {
        qOptions :: [QueryOption],
        qFullCollection :: FullCollection,
        qSkip :: Int32,  -- ^ Number of initial matching documents to skip
        qBatchSize :: Int32,  -- ^ The number of document to return in each batch response from the server. 0 means use Mongo default. Negative means close cursor after first batch and use absolute value as batch size.
        qSelector :: Document,  -- ^ \[\] = return all documents in collection
        qProjector :: Document  -- ^ \[\] = return whole document
    } | GetMore {
        gFullCollection :: FullCollection,
        gBatchSize :: Int32,
        gCursorId :: CursorId}
    deriving (Show, Eq)

data QueryOption =
      TailableCursor  -- ^ Tailable means cursor is not closed when the last data is retrieved. Rather, the cursor marks the final object's position. You can resume using the cursor later, from where it was located, if more data were received. Like any "latent cursor", the cursor may become invalid at some point – for example if the final object it references were deleted. Thus, you should be prepared to requery on CursorNotFound exception.
    | SlaveOK  -- ^ Allow query of replica slave. Normally these return an error except for namespace "local".
    | NoCursorTimeout  -- ^ The server normally times out idle cursors after 10 minutes to prevent a memory leak in case a client forgets to close a cursor. Set this option to allow a cursor to live forever until it is closed.
    | AwaitData  -- ^ Use with TailableCursor. If we are at the end of the data, block for a while rather than returning no data. After a timeout period, we do return as normal.
--  | Exhaust  -- ^ Stream the data down full blast in multiple "more" packages, on the assumption that the client will fully read all data queried. Faster when you are pulling a lot of data and know you want to pull it all down. Note: the client is not allowed to not read all the data unless it closes the connection.
-- Exhaust commented out because not compatible with current `Pipeline` implementation
    | Partial  -- ^ Get partial results from a _mongos_ if some shards are down, instead of throwing an error.
    deriving (Show, Eq)

-- *** Binary format

qOpcode :: Request -> Opcode
qOpcode Query{} = 2004
qOpcode GetMore{} = 2005

putRequest :: Request -> RequestId -> Put
putRequest request requestId = do
    putHeader (qOpcode request) requestId
    case request of
        Query{..} -> do
            putInt32 (qBits qOptions)
            putCString qFullCollection
            putInt32 qSkip
            putInt32 qBatchSize
            putDocument qSelector
            unless (null qProjector) (putDocument qProjector)
        GetMore{..} -> do
            putInt32 0
            putCString gFullCollection
            putInt32 gBatchSize
            putInt64 gCursorId

qBit :: QueryOption -> Int32
qBit TailableCursor = bit 1
qBit SlaveOK = bit 2
qBit NoCursorTimeout = bit 4
qBit AwaitData = bit 5
--qBit Exhaust = bit 6
qBit Partial = bit 7

qBits :: [QueryOption] -> Int32
qBits = bitOr . map qBit

-- ** Reply

-- | A reply is a message received in response to a 'Request'
data Reply = Reply {
    rResponseFlags :: [ResponseFlag],
    rCursorId :: CursorId,  -- ^ 0 = cursor finished
    rStartingFrom :: Int32,
    rDocuments :: [Document]
    } deriving (Show, Eq)

data ResponseFlag =
      CursorNotFound  -- ^ Set when getMore is called but the cursor id is not valid at the server. Returned with zero results.
    | QueryError  -- ^ Query error. Returned with one document containing an "$err" field holding the error message.
    | AwaitCapable  -- ^ For backward compatability: Set when the server supports the AwaitData query option. if it doesn't, a replica slave client should sleep a little between getMore's
    deriving (Show, Eq, Enum)

-- * Binary format

replyOpcode :: Opcode
replyOpcode = 1

getReply :: Get (ResponseTo, Reply)
getReply = do
    (opcode, responseTo) <- getHeader
    unless (opcode == replyOpcode) $ fail $ "expected reply opcode (1) but got " ++ show opcode
    rResponseFlags <-  rFlags <$> getInt32
    rCursorId <- getInt64
    rStartingFrom <- getInt32
    numDocs <- fromIntegral <$> getInt32
    rDocuments <- replicateM numDocs getDocument
    return (responseTo, Reply{..})

rFlags :: Int32 -> [ResponseFlag]
rFlags bits = filter (testBit bits . rBit) [CursorNotFound ..]

rBit :: ResponseFlag -> Int
rBit CursorNotFound = 0
rBit QueryError = 1
rBit AwaitCapable = 3

-- * Authentication

type Username = Text
type Password = Text
type Nonce = Text

pwHash :: Username -> Password -> Text
pwHash u p = T.pack . byteStringHex . MD5.hash . TE.encodeUtf8 $ u `T.append` ":mongo:" `T.append` p

pwKey :: Nonce -> Username -> Password -> Text
pwKey n u p = T.pack . byteStringHex . MD5.hash . TE.encodeUtf8 . T.append n . T.append u $ pwHash u p


{- Authors: Tony Hannan <tony@10gen.com>
   Copyright 2011 10gen Inc.
   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. -}
