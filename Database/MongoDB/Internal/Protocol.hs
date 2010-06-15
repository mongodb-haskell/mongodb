{-| Low-level messaging between this client and the MongoDB server. See Mongo Wire Protocol (<http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol>).

This module is not intended for direct use. Use the high-level interface at "Database.MongoDB.Query" instead. -}

{-# LANGUAGE RecordWildCards, StandaloneDeriving, OverloadedStrings #-}

module Database.MongoDB.Internal.Protocol (
	-- * FullCollection
	FullCollection,
	-- * Write
	Insert(..), insert,
	Update(..), UpdateOption(..), update,
	Delete(..), DeleteOption(..), delete,
	-- * Read
	Query(..), QueryOption(..), query,
	GetMore(..), getMore,
	-- ** Reply
	Reply(..),
	-- ** Cursor
	CursorId, killCursors,
	-- * Authentication
	Username, Password, Nonce, pwHash, pwKey
) where

import Prelude as P
import Database.MongoDB.Internal.Connection (Op, sendBytes, flushBytes, receiveBytes)
import Data.Bson
import Data.Bson.Binary
import Data.UString as U (pack, append, toByteString)
import Data.ByteString.Lazy as B (length, append)
import Data.Binary.Put
import Data.Binary.Get
import Data.Int
import Data.Bits
import Control.Monad.Reader
import Control.Applicative ((<$>))
import Data.IORef
import System.IO.Unsafe (unsafePerformIO)
import Data.Digest.OpenSSL.MD5 (md5sum)
import Database.MongoDB.Util (bitOr, (<.>))

-- * Authentication

type Username = UString
type Password = UString
type Nonce = UString

pwHash :: Username -> Password -> UString
pwHash u p = pack . md5sum . toByteString $ u `U.append` ":mongo:" `U.append` p

pwKey :: Nonce -> Username -> Password -> UString
pwKey n u p = pack . md5sum . toByteString . U.append n . U.append u $ pwHash u p

-- * FullCollection

type FullCollection = UString
-- ^ Database name and collection name with period (.) in between. Eg. \"myDb.myCollection\"

-- * Request / response

insert :: Insert -> Op ()
-- ^ Insert documents into collection
insert = send_ . putInsert

update :: Update -> Op ()
-- ^ Update documents in collection matching selector using updater
update = send_ . putUpdate

delete :: Delete -> Op ()
-- ^ Delete documents in collection matching selector
delete = send_ . putDelete

killCursors :: [CursorId] -> Op ()
-- ^ Close cursors on server because we will not be getting anymore documents from them
killCursors = send_ . putKillCursors . KillCursors

query :: Query -> Op Reply
-- ^ Return first batch of documents in collection matching selector and a cursor-id for getting remaining documents (see 'getMore')
query q = do
	requestId <- send (putQuery q)
	(reply, responseTo) <- receive getReply
	unless (responseTo == requestId) $ fail "expected response id to match query request id"
	return reply

getMore :: GetMore -> Op Reply
-- ^ Get next batch of documents from cursor
getMore g = do
	requestId <- send (putGetMore g)
	(reply, responseTo) <- receive getReply
	unless (responseTo == requestId) $ fail "expected response id to match get-more request id"
	return reply

-- ** Send / receive

type RequestId = Int32
-- ^ A fresh request id is generated for every message

genRequestId :: IO RequestId
-- ^ Generate fresh request id
genRequestId = atomicModifyIORef counter $ \n -> (n + 1, n) where
	counter :: IORef RequestId
	counter = unsafePerformIO (newIORef 0)
	{-# NOINLINE counter #-}

type ResponseTo = RequestId

send_ :: (RequestId -> Put) -> Op ()
send_ x = send x >> return ()

send :: (RequestId -> Put) -> Op RequestId
send rput = do
	requestId <- liftIO genRequestId
	let bytes = runPut (rput requestId)
	let lengthBytes = runPut . putInt32 $ (toEnum . fromEnum) (B.length bytes + 4)
	sendBytes (B.append lengthBytes bytes)
	flushBytes
	return requestId

receive :: Get a -> Op a
receive getMess = do
	messageLength <- fromIntegral . runGet getInt32 <$> receiveBytes 4
	runGet getMess <$> receiveBytes (messageLength - 4)

-- * Messages

data Insert = Insert {
	iFullCollection :: FullCollection,
	iDocuments :: [Document]
	} deriving (Show, Eq)

data Update = Update {
	uFullCollection :: FullCollection,
	uOptions :: [UpdateOption],
	uSelector :: Document,
	uUpdater :: Document
	} deriving (Show, Eq)

data UpdateOption =
	  Upsert  -- ^ If set, the database will insert the supplied object into the collection if no matching document is found
	| MultiUpdate  -- ^ If set, the database will update all matching objects in the collection. Otherwise only updates first matching doc
	deriving (Show, Eq)

data Delete = Delete {
	dFullCollection :: FullCollection,
	dOptions :: [DeleteOption],
	dSelector :: Document
	} deriving (Show, Eq)

data DeleteOption = SingleRemove  -- ^ If set, the database will remove only the first matching document in the collection. Otherwise all matching documents will be removed
	deriving (Show, Eq)

data Query = Query {
	qOptions :: [QueryOption],
	qFullCollection :: FullCollection,
	qSkip :: Int32,  -- ^ Number of initial matching documents to skip
	qBatchSize :: Int32,  -- ^ The number of document to return in each batch response from the server. 0 means use Mongo default. Negative means close cursor after first batch and use absolute value as batch size.
	qSelector :: Document,  -- ^ \[\] = return all documents in collection
	qProjector :: Document  -- ^ \[\] = return whole document
	} deriving (Show, Eq)

data QueryOption =
	TailableCursor |
	SlaveOK |
	NoCursorTimeout
	deriving (Show, Eq)

data GetMore = GetMore {
	gFullCollection :: FullCollection,
	gBatchSize :: Int32,
	gCursorId :: CursorId
	} deriving (Show, Eq)

newtype KillCursors = KillCursors {
	kCursorIds :: [CursorId]
	} deriving (Show, Eq)

data Reply = Reply {
	rResponseFlag :: Int32,  -- ^ 0 = success, non-zero = failure
	rCursorId :: CursorId,  -- ^ 0 = cursor finished
	rStartingFrom :: Int32,
	rDocuments :: [Document]
	} deriving (Show, Eq)

type CursorId = Int64

-- ** Messages binary format

type Opcode = Int32
-- ^ Code for each message type
replyOpcode, updateOpcode, insertOpcode, queryOpcode, getMoreOpcode, deleteOpcode, killCursorsOpcode :: Opcode
replyOpcode = 1
updateOpcode = 2001
insertOpcode = 2002
queryOpcode = 2004
getMoreOpcode = 2005
deleteOpcode = 2006
killCursorsOpcode = 2007

putUpdate :: Update -> RequestId -> Put
putUpdate Update{..} = putMessage updateOpcode $ do
	putInt32 0
	putCString uFullCollection
	putInt32 (uBits uOptions)
	putDocument uSelector
	putDocument uUpdater

uBit :: UpdateOption -> Int32
uBit Upsert = bit 0
uBit MultiUpdate = bit 1

uBits :: [UpdateOption] -> Int32
uBits = bitOr . map uBit

putInsert :: Insert -> RequestId -> Put
putInsert Insert{..} = putMessage insertOpcode $ do
	putInt32 0
	putCString iFullCollection
	mapM_ putDocument iDocuments

putDelete :: Delete -> RequestId -> Put
putDelete Delete{..} = putMessage deleteOpcode $ do
	putInt32 0
	putCString dFullCollection
	putInt32 (dBits dOptions)
	putDocument dSelector

dBit :: DeleteOption -> Int32
dBit SingleRemove = bit 0

dBits :: [DeleteOption] -> Int32
dBits = bitOr . map dBit

putQuery :: Query -> RequestId -> Put
putQuery Query{..} = putMessage queryOpcode $ do
	putInt32 (qBits qOptions)
	putCString qFullCollection
	putInt32 qSkip
	putInt32 qBatchSize
	putDocument qSelector
	unless (null qProjector) (putDocument qProjector)

qBit :: QueryOption -> Int32
qBit TailableCursor = bit 1
qBit SlaveOK = bit 2
qBit NoCursorTimeout = bit 4

qBits :: [QueryOption] -> Int32
qBits = bitOr . map qBit

putGetMore :: GetMore -> RequestId -> Put
putGetMore GetMore{..} = putMessage getMoreOpcode $ do
	putInt32 0
	putCString gFullCollection
	putInt32 gBatchSize
	putInt64 gCursorId

putKillCursors :: KillCursors -> RequestId -> Put
putKillCursors KillCursors{..} = putMessage killCursorsOpcode $ do
	putInt32 0
	putInt32 $ toEnum (P.length kCursorIds)
	mapM_ putInt64 kCursorIds

getReply :: Get (Reply, ResponseTo)
getReply = getMessage replyOpcode $ do
	rResponseFlag <- getInt32
	rCursorId <- getInt64
	rStartingFrom <- getInt32
	numDocs <- getInt32
	rDocuments <- replicateM (fromIntegral numDocs) getDocument
	return $ Reply {..}

-- *** Message header

putMessage :: Opcode -> Put -> RequestId -> Put
-- ^ Note, does not write message length (first int32), assumes caller will write it
putMessage opcode messageBodyPut requestId = do
	putInt32 requestId
	putInt32 0
	putInt32 opcode
	messageBodyPut

getMessage :: Opcode -> Get a -> Get (a, ResponseTo)
-- ^ Note, does not read message length (first int32), assumes it was already read
getMessage expectedOpcode getMessageBody = do
	_requestId <- getInt32
	responseTo <- getInt32
	opcode <- getInt32
	unless (opcode == expectedOpcode) $
		fail $ "expected opcode " ++ show expectedOpcode ++ " but got " ++ show opcode
	body <- getMessageBody
	return (body, responseTo)


{- Authors: Tony Hannan <tony@10gen.com>
   Copyright 2010 10gen Inc.
   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. -}
