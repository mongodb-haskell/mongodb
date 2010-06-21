{-| Low-level messaging between this client and the MongoDB server. See Mongo Wire Protocol (<http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol>).

This module is not intended for direct use. Use the high-level interface at "Database.MongoDB.Query" instead. -}

{-# LANGUAGE RecordWildCards, StandaloneDeriving, OverloadedStrings #-}

module Database.MongoDB.Internal.Protocol (
	-- * Connection
	Connection, mkConnection,
	send, call,
	-- * Message
	FullCollection,
	-- ** Notice
	Notice(..), UpdateOption(..), DeleteOption(..), CursorId,
	-- ** Request
	Request(..), QueryOption(..),
	-- ** Reply
	Reply(..),
	-- * Authentication
	Username, Password, Nonce, pwHash, pwKey
) where

import Prelude as X
import Control.Applicative ((<$>))
import Control.Monad (unless, replicateM)
import System.IO (Handle)
import Data.ByteString.Lazy (ByteString)
import qualified Control.Pipeline as P
import Data.Bson (Document, UString)
import Data.Bson.Binary
import Data.Binary.Put
import Data.Binary.Get
import Data.Int
import Data.Bits
import Database.MongoDB.Internal.Util (bitOr)
import Data.IORef
import System.IO.Unsafe (unsafePerformIO)
import Data.Digest.OpenSSL.MD5 (md5sum)
import Data.UString as U (pack, append, toByteString)

-- * Connection

type Connection = P.Pipe Handle ByteString
-- ^ Thread-safe TCP connection to server with pipelined requests

mkConnection :: Handle -> IO Connection
-- ^ New thread-safe pipelined connection over handle
mkConnection = P.newPipe encodeSize decodeSize where
	encodeSize = runPut . putInt32 . toEnum . (+ 4)
	decodeSize = subtract 4 . fromEnum . runGet getInt32

send :: Connection -> [Notice] -> IO ()
-- ^ Send notices as a contiguous batch to server with no reply. Raise IOError if connection fails.
send conn notices = P.send conn =<< mapM noticeBytes notices

call :: Connection -> [Notice] -> Request -> IO (IO Reply)
-- ^ Send notices and request as a contiguous batch to server and return reply promise, which will block when invoked until reply arrives. This call and resulting promise will raise IOError if connection fails.
call conn notices request = do
	nMessages <- mapM noticeBytes notices
	requestId <- genRequestId
	let rMessage = runPut (putRequest request requestId)
	promise <- P.call conn (nMessages ++ [rMessage])
	return (bytesReply requestId <$> promise)

noticeBytes :: Notice -> IO ByteString
noticeBytes notice = runPut . putNotice notice <$> genRequestId

bytesReply :: RequestId -> ByteString -> Reply
bytesReply requestId bytes = if requestId == responseTo then reply else err where
	(responseTo, reply) = runGet getReply bytes
	err = error $ "expected response id (" ++ show responseTo ++ ") to match request id (" ++ show requestId ++ ")"

-- * Messages

type FullCollection = UString
-- ^ Database name and collection name with period (.) in between. Eg. \"myDb.myCollection\"

-- ** Header

type Opcode = Int32

type RequestId = Int32
-- ^ A fresh request id is generated for every message

type ResponseTo = RequestId

genRequestId :: IO RequestId
-- ^ Generate fresh request id
genRequestId = atomicModifyIORef counter $ \n -> (n + 1, n) where
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
	putInt32 0
	case notice of
		Insert{..} -> do
			putCString iFullCollection
			mapM_ putDocument iDocuments
		Update{..} -> do
			putCString uFullCollection
			putInt32 (uBits uOptions)
			putDocument uSelector
			putDocument uUpdater
		Delete{..} -> do
			putCString dFullCollection
			putInt32 (dBits dOptions)
			putDocument dSelector
		KillCursors{..} -> do
			putInt32 $ toEnum (X.length kCursorIds)
			mapM_ putInt64 kCursorIds

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

-- | A request is a message that is sent with a 'Reply' returned
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
	TailableCursor |
	SlaveOK |
	NoCursorTimeout  -- Never timeout the cursor. When not set, the cursor will die if idle for more than 10 minutes.
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

qBits :: [QueryOption] -> Int32
qBits = bitOr . map qBit

-- ** Reply

-- | A reply is a message received in response to a 'Request'
data Reply = Reply {
	rResponseFlag :: Int32,  -- ^ 0 = success, non-zero = failure
	rCursorId :: CursorId,  -- ^ 0 = cursor finished
	rStartingFrom :: Int32,
	rDocuments :: [Document]
	} deriving (Show, Eq)

-- * Binary format

replyOpcode :: Opcode
replyOpcode = 1

getReply :: Get (ResponseTo, Reply)
getReply = do
	(opcode, responseTo) <- getHeader
	unless (opcode == replyOpcode) $ fail $ "expected reply opcode (1) but got " ++ show opcode
	rResponseFlag <- getInt32
	rCursorId <- getInt64
	rStartingFrom <- getInt32
	numDocs <- fromIntegral <$> getInt32
	rDocuments <- replicateM numDocs getDocument
	return (responseTo, Reply{..})

-- * Authentication

type Username = UString
type Password = UString
type Nonce = UString

pwHash :: Username -> Password -> UString
pwHash u p = pack . md5sum . toByteString $ u `U.append` ":mongo:" `U.append` p

pwKey :: Nonce -> Username -> Password -> UString
pwKey n u p = pack . md5sum . toByteString . U.append n . U.append u $ pwHash u p
