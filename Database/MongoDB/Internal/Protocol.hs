-- | Low-level messaging between this client and the MongoDB server, see Mongo
-- Wire Protocol (<http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol>).
--
-- This module is not intended for direct use. Use the high-level interface at
-- "Database.MongoDB.Query" and "Database.MongoDB.Connection" instead.

{-# LANGUAGE RecordWildCards, OverloadedStrings #-}
{-# LANGUAGE CPP, FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses, FlexibleInstances, UndecidableInstances #-}
{-# LANGUAGE BangPatterns #-}

{-# LANGUAGE NamedFieldPuns, ScopedTypeVariables #-}

#if (__GLASGOW_HASKELL__ >= 706)
{-# LANGUAGE RecursiveDo #-}
#else
{-# LANGUAGE DoRec #-}
#endif

module Database.MongoDB.Internal.Protocol (
    FullCollection,
    -- * Pipe
    Pipe,  newPipe, newPipeWith, send, sendOpMsg, call, callOpMsg,
    -- ** Notice
    Notice(..), InsertOption(..), UpdateOption(..), DeleteOption(..), CursorId,
    -- ** Request
    Request(..), QueryOption(..), Cmd (..), KillC(..),
    -- ** Reply
    Reply(..), ResponseFlag(..), FlagBit(..),
    -- * Authentication
    Username, Password, Nonce, pwHash, pwKey,
    isClosed, close, ServerData(..), Pipeline(..), putOpMsg,
    bitOpMsg
) where

#if !MIN_VERSION_base(4,8,0)
import Control.Applicative ((<$>))
#endif
import Control.Monad ( forM, replicateM, unless, forever )
import Data.Binary.Get (Get, runGet, getInt8)
import Data.Binary.Put (Put, runPut, putInt8)
import Data.Bits (bit, testBit, zeroBits)
import Data.Int (Int32, Int64)
import Data.IORef (IORef, newIORef, atomicModifyIORef)
import System.IO (Handle)
import System.IO.Error (doesNotExistErrorType, mkIOError)
import System.IO.Unsafe (unsafePerformIO)
import Data.Maybe (maybeToList, fromJust)
import GHC.Conc (ThreadStatus(..), threadStatus)
import Control.Monad.STM (atomically)
import Control.Concurrent (ThreadId, killThread, forkIOWithUnmask)
import Control.Concurrent.STM.TChan (TChan, newTChan, readTChan, writeTChan, isEmptyTChan)

import Control.Exception.Lifted (SomeException, mask_, onException, throwIO, try)

import qualified Data.ByteString.Lazy as L

import Control.Monad.Trans (MonadIO, liftIO)
import Data.Bson (Document, (=:), merge, cast, valueAt, look)
import Data.Bson.Binary (getDocument, putDocument, getInt32, putInt32, getInt64,
                         putInt64, putCString)
import Data.Text (Text)

import qualified Crypto.Hash.MD5 as MD5
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE

import Database.MongoDB.Internal.Util (bitOr, byteStringHex)

import Database.MongoDB.Transport (Transport)
import qualified Database.MongoDB.Transport as Tr


#if MIN_VERSION_base(4,6,0)
import Control.Concurrent.MVar.Lifted (MVar, newEmptyMVar, newMVar, withMVar,
                                       putMVar, readMVar, mkWeakMVar, isEmptyMVar)
import GHC.List (foldl1')
import Conduit (repeatWhileMC, (.|), runConduit, foldlC)
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
data Pipeline = Pipeline
    { vStream :: MVar Transport -- ^ Mutex on handle, so only one thread at a time can write to it
    , responseQueue :: TChan (MVar (Either IOError Response)) -- ^ Queue of threads waiting for responses. Every time a response arrives we pop the next thread and give it the response.
    , listenThread :: ThreadId
    , finished :: MVar ()
    , serverData :: ServerData
    }

data ServerData = ServerData
                { isMaster            :: Bool
                , minWireVersion      :: Int
                , maxWireVersion      :: Int
                , maxMessageSizeBytes :: Int
                , maxBsonObjectSize   :: Int
                , maxWriteBatchSize   :: Int
                }
                deriving Show

-- | @'forkUnmaskedFinally' action and_then@ behaves the same as @'forkFinally' action and_then@, except that @action@ is run completely unmasked, whereas with 'forkFinally', @action@ is run with the same mask as the parent thread.
forkUnmaskedFinally :: IO a -> (Either SomeException a -> IO ()) -> IO ThreadId
forkUnmaskedFinally action and_then =
  mask_ $ forkIOWithUnmask $ \unmask ->
    try (unmask action) >>= and_then

-- | Create new Pipeline over given handle. You should 'close' pipeline when finished, which will also close handle. If pipeline is not closed but eventually garbage collected, it will be closed along with handle.
newPipeline :: ServerData -> Transport -> IO Pipeline
newPipeline serverData stream = do
    vStream <- newMVar stream
    responseQueue <- atomically newTChan
    finished <- newEmptyMVar
    let drainReplies = do
          chanEmpty <- atomically $ isEmptyTChan responseQueue
          if chanEmpty
            then return ()
            else do
              var <- atomically $ readTChan responseQueue
              putMVar var $ Left $ mkIOError
                                        doesNotExistErrorType
                                        "Handle has been closed"
                                        Nothing
                                        Nothing
              drainReplies

    rec
        let pipe = Pipeline{..}
        listenThread <- forkUnmaskedFinally (listen pipe) $ \_ -> do
                                                              putMVar finished ()
                                                              drainReplies

    _ <- mkWeakMVar vStream $ do
        killThread listenThread
        Tr.close stream
    return pipe

isFinished :: Pipeline -> IO Bool
isFinished Pipeline {finished} = do
  empty <- isEmptyMVar finished
  return $ not empty

close :: Pipeline -> IO ()
-- ^ Close pipe and underlying connection
close Pipeline{..} = do
    killThread listenThread
    Tr.close =<< readMVar vStream

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
        var <- atomically $ readTChan responseQueue
        putMVar var e
        case e of
            Left err -> Tr.close stream >> ioError err  -- close and stop looping
            Right _ -> return ()

psend :: Pipeline -> Message -> IO ()
-- ^ Send message to destination; the destination must not response (otherwise future 'call's will get these responses instead of their own).
-- Throw IOError and close pipeline if send fails
psend p@Pipeline{..} !message = withMVar vStream (flip writeMessage message) `onException` close p

psendOpMsg :: Pipeline -> [Cmd] -> Maybe FlagBit -> Document -> IO ()-- IO (IO Response)
psendOpMsg p@Pipeline{..} commands flagBit params =
  case flagBit of
    Just f -> case f of
               MoreToCome -> withMVar vStream (\t -> writeOpMsgMessage t (commands, Nothing) flagBit params) `onException` close p -- >> return (return (0, ReplyEmpty))
               _ -> error "moreToCome has to be set if no response is expected"
    _ -> error "moreToCome has to be set if no response is expected"

pcall :: Pipeline -> Message -> IO (IO Response)
-- ^ Send message to destination and return /promise/ of response from one message only. The destination must reply to the message (otherwise promises will have the wrong responses in them).
-- Throw IOError and closes pipeline if send fails, likewise for promised response.
pcall p@Pipeline{..} message = do
  listenerStopped <- isFinished p
  if listenerStopped
    then ioError $ mkIOError doesNotExistErrorType "Handle has been closed" Nothing Nothing
    else withMVar vStream doCall `onException` close p
  where
    doCall stream = do
        writeMessage stream message
        var <- newEmptyMVar
        liftIO $ atomically $ writeTChan responseQueue var
        return $ readMVar var >>= either throwIO return -- return promise

pcallOpMsg :: Pipeline -> Maybe (Request, RequestId) -> Maybe FlagBit -> Document -> IO (IO Response)
-- ^ Send message to destination and return /promise/ of response from one message only. The destination must reply to the message (otherwise promises will have the wrong responses in them).
-- Throw IOError and closes pipeline if send fails, likewise for promised response.
pcallOpMsg p@Pipeline{..} message flagbit params = do
  listenerStopped <- isFinished p
  if listenerStopped
    then ioError $ mkIOError doesNotExistErrorType "Handle has been closed" Nothing Nothing
    else withMVar vStream doCall `onException` close p
  where
    doCall stream = do
        writeOpMsgMessage stream ([], message) flagbit params
        var <- newEmptyMVar
        -- put var into the response-queue so that it can
        -- fetch the latest response
        liftIO $ atomically $ writeTChan responseQueue var
        return $ readMVar var >>= either throwIO return -- return promise

-- * Pipe

type Pipe = Pipeline
-- ^ Thread-safe TCP connection with pipelined requests. In long-running applications the user is expected to use it as a "client": create a `Pipe`
-- at startup, use it as long as possible, watch out for possible timeouts, and close it on shutdown. Bearing in mind that disconnections may be triggered by MongoDB service providers, the user is responsible for re-creating their `Pipe` whenever necessary.

newPipe :: ServerData -> Handle -> IO Pipe
-- ^ Create pipe over handle
newPipe sd handle = Tr.fromHandle handle >>= (newPipeWith sd)

newPipeWith :: ServerData -> Transport -> IO Pipe
-- ^ Create pipe over connection
newPipeWith sd conn = newPipeline sd conn

send :: Pipe -> [Notice] -> IO ()
-- ^ Send notices as a contiguous batch to server with no reply. Throw IOError if connection fails.
send pipe notices = psend pipe (notices, Nothing)

sendOpMsg :: Pipe -> [Cmd] -> Maybe FlagBit -> Document -> IO ()
-- ^ Send notices as a contiguous batch to server with no reply. Throw IOError if connection fails.
sendOpMsg pipe commands@(Nc _ : _) flagBit params =  psendOpMsg pipe commands flagBit params
sendOpMsg pipe commands@(Kc _ : _) flagBit params =  psendOpMsg pipe commands flagBit params
sendOpMsg _ _ _ _ =  error "This function only supports Cmd types wrapped in Nc or Kc type constructors"

call :: Pipe -> [Notice] -> Request -> IO (IO Reply)
-- ^ Send notices and request as a contiguous batch to server and return reply promise, which will block when invoked until reply arrives. This call and resulting promise will throw IOError if connection fails.
call pipe notices request = do
    requestId <- genRequestId
    promise <- pcall pipe (notices, Just (request, requestId))
    return $ check requestId <$> promise
 where
    check requestId (responseTo, reply) = if requestId == responseTo then reply else
        error $ "expected response id (" ++ show responseTo ++ ") to match request id (" ++ show requestId ++ ")"

callOpMsg :: Pipe -> Request -> Maybe FlagBit -> Document -> IO (IO Reply)
-- ^ Send requests as a contiguous batch to server and return reply promise, which will block when invoked until reply arrives. This call and resulting promise will throw IOError if connection fails.
callOpMsg pipe request flagBit params = do
    requestId <- genRequestId
    promise <- pcallOpMsg pipe (Just (request, requestId)) flagBit params
    promise' <- promise :: IO Response
    return $ snd <$> produce requestId promise'
 where
   -- We need to perform streaming here as within the OP_MSG protocol mongoDB expects
   -- our client to keep receiving messages after the MoreToCome flagbit was
   -- set by the server until our client receives an empty flagbit. After the
   -- first MoreToCome flagbit was set the responseTo field in the following
   -- headers will reference the cursorId that was set in the previous message.
   -- see:
   -- https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.rst#moretocome-on-responses
    checkFlagBit p =
      case p of
        (_, r) ->
          case r of
            ReplyOpMsg{..} -> flagBits == [MoreToCome]
             -- This is called by functions using the OP_MSG protocol,
             -- so this has to be ReplyOpMsg
            _ -> error "Impossible"
    produce reqId p = runConduit $
      case p of
        (rt, r) ->
          case r of
              ReplyOpMsg{..} ->
                if flagBits == [MoreToCome]
                  then yieldResponses .| foldlC mergeResponses p
                  else return $ (rt, check reqId p)
              _ -> error "Impossible" -- see comment above
    yieldResponses = repeatWhileMC
          (do
             var <- newEmptyMVar
             liftIO $ atomically $ writeTChan (responseQueue pipe) var
             readMVar var >>= either throwIO return :: IO Response
          )
          checkFlagBit
    mergeResponses p@(rt,rep) p' =
      case (p, p') of
          ((_, r), (_, r')) ->
            case (r, r') of
                (ReplyOpMsg _ sec _, ReplyOpMsg _ sec' _) -> do
                    let (section, section') = (head sec, head sec')
                        (cur, cur') = (maybe Nothing cast $ look "cursor" section,
                                      maybe Nothing cast $ look "cursor" section')
                    case (cur, cur') of
                      (Just doc, Just doc') -> do
                        let (docs, docs') =
                              ( fromJust $ cast $ valueAt "nextBatch" doc :: [Document]
                              , fromJust $ cast $ valueAt "nextBatch" doc' :: [Document])
                            id' = fromJust $ cast $ valueAt "id" doc' :: Int32
                        (rt, check id' (rt, rep{ sections = docs' ++ docs })) -- todo: avoid (++)
                        -- Since we use this to process moreToCome messages, we
                        -- know that there will be a nextBatch key in the document
                      _ ->  error "Impossible"
                _ -> error "Impossible" -- see comment above
    check requestId (responseTo, reply) = if requestId == responseTo then reply else
        error $ "expected response id (" ++ show responseTo ++ ") to match request id (" ++ show requestId ++ ")"

-- * Message

type Message = ([Notice], Maybe (Request, RequestId))
-- ^ A write notice(s) with getLastError request, or just query request.
-- Note, that requestId will be out of order because request ids will be generated for notices after the request id supplied was generated. This is ok because the mongo server does not care about order just uniqueness.
type OpMsgMessage = ([Cmd], Maybe (Request, RequestId))

writeMessage :: Transport -> Message -> IO ()
-- ^ Write message to connection
writeMessage conn (notices, mRequest) = do
    noticeStrings <- forM notices $ \n -> do
          requestId <- genRequestId
          let s = runPut $ putNotice n requestId
          return $ (lenBytes s) `L.append` s

    let requestString = do
          (request, requestId) <- mRequest
          let s = runPut $ putRequest request requestId
          return $ (lenBytes s) `L.append` s

    Tr.write conn $ L.toStrict $ L.concat $ noticeStrings ++ (maybeToList requestString)
    Tr.flush conn
 where
    lenBytes bytes = encodeSize . toEnum . fromEnum $ L.length bytes
    encodeSize = runPut . putInt32 . (+ 4)

writeOpMsgMessage :: Transport -> OpMsgMessage -> Maybe FlagBit -> Document -> IO ()
-- ^ Write message to connection
writeOpMsgMessage conn (notices, mRequest) flagBit params = do
    noticeStrings <- forM notices $ \n -> do
          requestId <- genRequestId
          let s = runPut $ putOpMsg n requestId flagBit params
          return $ (lenBytes s) `L.append` s

    let requestString = do
           (request, requestId) <- mRequest
           let s = runPut $ putOpMsg (Req request) requestId flagBit params
           return $ (lenBytes s) `L.append` s

    Tr.write conn $ L.toStrict $ L.concat $ noticeStrings ++ (maybeToList requestString)
    Tr.flush conn
 where
    lenBytes bytes = encodeSize . toEnum . fromEnum $ L.length bytes
    encodeSize = runPut . putInt32 . (+ 4)

type Response = (ResponseTo, Reply)
-- ^ Message received from a Mongo server in response to a Request

readMessage :: Transport -> IO Response
-- ^ read response from a connection
readMessage conn = readResp  where
    readResp = do
        len <- fromEnum . decodeSize . L.fromStrict <$> Tr.read conn 4
        runGet getReply . L.fromStrict <$> Tr.read conn len
    decodeSize = subtract 4 . runGet getInt32

type FullCollection = Text
-- ^ Database name and collection name with period (.) in between. Eg. \"myDb.myCollection\"

-- ** Header

type Opcode = Int32

type RequestId = Int32
-- ^ A fresh request id is generated for every message

type ResponseTo = RequestId

genRequestId :: (MonadIO m) => m RequestId
-- ^ Generate fresh request id
{-# NOINLINE genRequestId #-}
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

putOpMsgHeader :: Opcode -> RequestId -> Put
-- ^ Note, does not write message length (first int32), assumes caller will write it
putOpMsgHeader opcode requestId = do
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

data KillC = KillC { killCursor :: Notice, kFullCollection:: FullCollection} deriving Show

data Cmd = Nc Notice | Req Request | Kc KillC deriving Show

data FlagBit =
      ChecksumPresent  -- ^ The message ends with 4 bytes containing a CRC-32C checksum
    | MoreToCome  -- ^ Another message will follow this one without further action from the receiver.
    | ExhaustAllowed  -- ^ The client is prepared for multiple replies to this request using the moreToCome bit.
    deriving (Show, Eq, Enum)

uOptDoc :: UpdateOption -> Document
uOptDoc Upsert = ["upsert" =: True]
uOptDoc MultiUpdate = ["multi" =: True]

{-
  OP_MSG header == 16 byte
  + 4 bytes flagBits
  + 1 byte payload type = 1
  + 1 byte payload type = 2
  + 4 byte size of payload
  == 26 bytes opcode overhead
  + X Full command document {insert: "test", writeConcern: {...}}
  + Y command identifier ("documents", "deletes", "updates") ( + \0)
-}
putOpMsg :: Cmd -> RequestId -> Maybe FlagBit -> Document -> Put
putOpMsg cmd requestId flagBit params = do
    let biT = maybe zeroBits (bit . bitOpMsg) flagBit:: Int32
    putOpMsgHeader opMsgOpcode requestId -- header
    case cmd of
        Nc n -> case n of
            Insert{..} -> do
                let (sec0, sec1Size) =
                      prepSectionInfo
                          iFullCollection
                          (Just (iDocuments:: [Document]))
                          (Nothing:: Maybe Document)
                          ("insert":: Text)
                          ("documents":: Text)
                          params
                putInt32 biT                         -- flagBit
                putInt8 0                            -- payload type 0
                putDocument sec0                     -- payload
                putInt8 1                            -- payload type 1
                putInt32 sec1Size                    -- size of section
                putCString "documents"               -- identifier
                mapM_ putDocument iDocuments         -- payload
            Update{..} -> do
                let doc = ["q" =: uSelector, "u" =: uUpdater] <> concatMap uOptDoc uOptions
                    (sec0, sec1Size) =
                      prepSectionInfo
                          uFullCollection
                          (Nothing:: Maybe [Document])
                          (Just doc)
                          ("update":: Text)
                          ("updates":: Text)
                          params
                putInt32 biT
                putInt8 0
                putDocument sec0
                putInt8 1
                putInt32 sec1Size
                putCString "updates"
                putDocument doc
            Delete{..} -> do
                -- Setting limit to 1 here is ok, since this is only used by deleteOne
                let doc = ["q" =: dSelector, "limit" =: (1 :: Int32)]
                    (sec0, sec1Size) =
                      prepSectionInfo
                          dFullCollection
                          (Nothing:: Maybe [Document])
                          (Just doc)
                          ("delete":: Text)
                          ("deletes":: Text)
                          params
                putInt32 biT
                putInt8 0
                putDocument sec0
                putInt8 1
                putInt32 sec1Size
                putCString "deletes"
                putDocument doc
            _ -> error "The KillCursors command cannot be wrapped into a Nc type constructor. Please use the Kc type constructor"
        Req r -> case r of
            Query{..} -> do
                let n = T.splitOn "." qFullCollection
                    db = head n
                    sec0 = foldl1' merge [qProjector, [ "$db" =: db ], qSelector]
                putInt32 biT
                putInt8 0
                putDocument sec0
            GetMore{..} -> do
                let n = T.splitOn "." gFullCollection
                    (db, coll) = (head n, last n)
                    pre = ["getMore" =: gCursorId, "collection" =: coll, "$db" =: db, "batchSize" =: gBatchSize]
                putInt32 (bit $ bitOpMsg $ ExhaustAllowed)
                putInt8 0
                putDocument pre
            Message{..} -> do
                putInt32 biT
                putInt8 0
                putDocument $ merge [ "$db" =: mDatabase ] mParams
        Kc k -> case k of
            KillC{..} -> do
                let n = T.splitOn "." kFullCollection
                    (db, coll) = (head n, last n)
                case killCursor of
                  KillCursors{..} -> do
                      let doc = ["killCursors" =: coll, "cursors" =: kCursorIds, "$db" =: db]
                      putInt32 biT
                      putInt8 0
                      putDocument doc
                  -- Notices are already captured at the beginning, so all
                  -- other cases are impossible
                  _ -> error "impossible"
 where
    lenBytes bytes = toEnum . fromEnum $ L.length bytes:: Int32
    prepSectionInfo fullCollection documents document command identifier ps =
      let n = T.splitOn "." fullCollection
          (db, coll) = (head n, last n)
      in
      case documents of
        Just ds ->
            let
                sec0 = merge ps [command =: coll, "$db" =: db]
                s = sum $ map (lenBytes . runPut . putDocument) ds
                i = runPut $ putCString identifier
                -- +4 bytes for the type 1 section size that has to be
                -- transported in addition to the type 1 section document
                sec1Size = s + lenBytes i + 4
            in (sec0, sec1Size)
        Nothing ->
            let
                sec0 = merge ps [command =: coll, "$db" =: db]
                s = runPut $ putDocument $ fromJust document
                i = runPut $ putCString identifier
                sec1Size = lenBytes s + lenBytes i + 4
            in (sec0, sec1Size)

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

bitOpMsg :: FlagBit -> Int
bitOpMsg ChecksumPresent = 0
bitOpMsg MoreToCome = 1
bitOpMsg ExhaustAllowed = 16

-- ** Request

-- | A request is a message that is sent with a 'Reply' expected in return
data Request =
      Query {
        qOptions :: [QueryOption],
        qFullCollection :: FullCollection,
        qSkip :: Int32,  -- ^ Number of initial matching documents to skip
        qBatchSize :: Int32,  -- ^ The number of document to return in each batch response from the server. 0 means use Mongo default. Negative means close cursor after first batch and use absolute value as batch size.
        qSelector :: Document,  -- ^ @[]@ = return all documents in collection
        qProjector :: Document  -- ^ @[]@ = return whole document
    } | GetMore {
        gFullCollection :: FullCollection,
        gBatchSize :: Int32,
        gCursorId :: CursorId
    } | Message {
        mDatabase :: Text,
        mParams :: Document
    }
    deriving (Show, Eq)

data QueryOption =
      TailableCursor  -- ^ Tailable means cursor is not closed when the last data is retrieved. Rather, the cursor marks the final object's position. You can resume using the cursor later, from where it was located, if more data were received. Like any "latent cursor", the cursor may become invalid at some point – for example if the final object it references were deleted. Thus, you should be prepared to requery on @CursorNotFound@ exception.
    | SlaveOK  -- ^ Allow query of replica slave. Normally these return an error except for namespace "local".
    | NoCursorTimeout  -- ^ The server normally times out idle cursors after 10 minutes to prevent a memory leak in case a client forgets to close a cursor. Set this option to allow a cursor to live forever until it is closed.
    | AwaitData  -- ^ Use with TailableCursor. If we are at the end of the data, block for a while rather than returning no data. After a timeout period, we do return as normal.

--  | Exhaust  -- ^ Stream the data down full blast in multiple "more" packages, on the assumption that the client will fully read all data queried. Faster when you are pulling a lot of data and know you want to pull it all down. Note: the client is not allowed to not read all the data unless it closes the connection.
-- Exhaust commented out because not compatible with current `Pipeline` implementation

    | Partial  -- ^ Get partial results from a /mongos/ if some shards are down, instead of throwing an error.
    deriving (Show, Eq)

-- *** Binary format

qOpcode :: Request -> Opcode
qOpcode Query{} = 2004
qOpcode GetMore{} = 2005
qOpcode Message{} = 2013

opMsgOpcode :: Opcode
opMsgOpcode = 2013

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
        Message{..} -> do
            putInt32 0
            putInt8 0
            putDocument $ merge [ "$db" =: mDatabase ] mParams

qBit :: QueryOption -> Int32
qBit TailableCursor = bit 1
qBit SlaveOK = bit 2
qBit NoCursorTimeout = bit 4
qBit AwaitData = bit 5
--qBit Exhaust = bit 6
qBit Database.MongoDB.Internal.Protocol.Partial = bit 7

qBits :: [QueryOption] -> Int32
qBits = bitOr . map qBit

-- ** Reply

-- | A reply is a message received in response to a 'Request'
data Reply = Reply {
    rResponseFlags :: [ResponseFlag],
    rCursorId :: CursorId,  -- ^ 0 = cursor finished
    rStartingFrom :: Int32,
    rDocuments :: [Document]
    }
   | ReplyOpMsg {
        flagBits :: [FlagBit],
        sections :: [Document],
        checksum :: Maybe Int32
    }
    deriving (Show, Eq)

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
    if opcode == 2013
      then do
            -- Notes:
            -- Checksum bits that are set by the server don't seem to be supported by official drivers.
            -- See: https://github.com/mongodb/mongo-python-driver/blob/master/pymongo/message.py#L1423
            flagBits <-  rFlagsOpMsg <$> getInt32
            _ <- getInt8
            sec0 <- getDocument
            let sections = [sec0]
                checksum = Nothing
            return (responseTo, ReplyOpMsg{..})
      else do
          unless (opcode == replyOpcode) $ fail $ "expected reply opcode (1) but got " ++ show opcode
          rResponseFlags <-  rFlags <$> getInt32
          rCursorId <- getInt64
          rStartingFrom <- getInt32
          numDocs <- fromIntegral <$> getInt32
          rDocuments <- replicateM numDocs getDocument
          return (responseTo, Reply{..})

rFlags :: Int32 -> [ResponseFlag]
rFlags bits = filter (testBit bits . rBit) [CursorNotFound ..]

-- See https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.rst#flagbits
rFlagsOpMsg :: Int32 -> [FlagBit]
rFlagsOpMsg bits = isValidFlag bits
  where isValidFlag bt =
          let setBits = map fst $ filter (\(_,b) -> b == True) $ zip ([0..31] :: [Int32]) $ map (testBit bt) [0 .. 31]
          in if any (\n -> not $ elem n [0,1,16]) setBits
               then error "Unsopported bit was set"
               else filter (testBit bt . bitOpMsg) [ChecksumPresent ..]

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
