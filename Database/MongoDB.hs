{-

Copyright (C) 2010 Scott R Parish <srp@srparish.net>

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

-}

module Database.MongoDB
    (
     -- * Connection
     Connection,
     connect, connectOnPort, conClose, disconnect,
     dropDatabase,
     -- * Database
     Database, MongoDBCollectionInvalid,
     ColCreateOpt(..),
     collectionNames, createCollection, dropCollection, validateCollection,
     -- * Collection
     Collection, FieldSelector, FullCollection,
     NumToSkip, NumToReturn, Selector,
     QueryOpt(..),
     UpdateFlag(..),
     count, countMatching, delete, insert, insertMany, query, remove, update,
     -- * Convience collection operations
     find, findOne, quickFind, quickFind',
     -- * Cursor
     Cursor,
     allDocs, allDocs', finish, nextDoc,
    )
where
import Control.Exception
import Control.Monad
import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.Bits
import Data.ByteString.Char8 hiding (count, find)
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy.UTF8 as L8
import Data.Int
import Data.IORef
import qualified Data.List as List
import Data.Maybe
import Data.Typeable
import Database.MongoDB.BSON as BSON
import Database.MongoDB.Util
import qualified Network
import Network.Socket hiding (connect, send, sendTo, recv, recvFrom)
import Prelude hiding (getContents)
import System.IO
import System.IO.Unsafe
import System.Random

-- | A handle to a database connection
data Connection = Connection { cHandle :: Handle, cRand :: IORef [Int] }

-- | Estabilish a connection to a MongoDB server
connect :: HostName -> IO Connection
connect = flip connectOnPort $ Network.PortNumber 27017

-- | Estabilish a connection to a MongoDB server on a non-standard port
connectOnPort :: HostName -> Network.PortID -> IO Connection
connectOnPort host port = do
  h <- Network.connectTo host port
  hSetBuffering h NoBuffering
  r <- newStdGen
  let ns = randomRs (fromIntegral (minBound :: Int32),
                     fromIntegral (maxBound :: Int32)) r
  nsRef <- newIORef ns
  return $ Connection { cHandle = h, cRand = nsRef }

-- | Close database connection
conClose :: Connection -> IO ()
conClose = hClose . cHandle

-- | Alias for 'conClose'
disconnect :: Connection -> IO ()
disconnect = conClose

-- | Drop a database.
dropDatabase :: Connection -> Database -> IO ()
dropDatabase c db = do
  _ <- dbCmd c db $ toBsonDoc [("dropDatabase", toBson (1::Int))]
  return ()

-- | Return a list of collections in /Database/.
collectionNames :: Connection -> Database -> IO [FullCollection]
collectionNames c db = do
  docs <- quickFind' c (db ++ ".system.namespaces") BSON.empty
  let names = flip List.map docs $ \doc ->
              fromBson $ fromJust $ BSON.lookup "name" doc
  return $ List.filter (List.notElem '$') names

data ColCreateOpt = CCOSize Int64  -- ^ Desired initial size for the
                                   -- collection (in bytes). must be
                                   -- less than or equal to
                                   -- 10000000000. For capped
                                   -- collections this size is the max
                                   -- size of the collection.
                  | CCOCapped Bool -- ^ If 'True', this is a capped collection.
                  | CCOMax Int64   -- ^ Maximum number of objects if capped.
                     deriving (Show, Eq)

colCreateOptToBson :: ColCreateOpt -> (String, BsonValue)
colCreateOptToBson (CCOSize sz) = ("size", toBson sz)
colCreateOptToBson (CCOCapped b) = ("capped", toBson b)
colCreateOptToBson (CCOMax m) = ("max", toBson m)

-- | Create a new collection in this database.
--
-- Normally collection creation is automatic. This function should
-- only be needed if you want to specify 'ColCreateOpt's on creation.
-- 'MongoDBCollectionInvalid' is thrown if the collection already
-- exists.
createCollection :: Connection -> FullCollection -> [ColCreateOpt] -> IO ()
createCollection c col opts = do
  let (db, col') = splitFullCol col
  dbcols <- collectionNames c db
  case col `List.elem` dbcols of
    True -> throwColInvalid $ "Collection already exists: " ++ show col
    False -> return ()
  case ".." `List.elem` (List.group col) of
    True -> throwColInvalid $ "Collection can't contain \"..\": " ++ show col
    False -> return ()
  case '$' `List.elem` col &&
       not ("oplog.$mail" `List.isPrefixOf` col' ||
            "$cmd" `List.isPrefixOf` col') of
    True -> throwColInvalid $ "Collection can't contain '$': " ++ show col
    False -> return ()
  case List.head col == '.' || List.last col == '.' of
    True -> throwColInvalid $
            "Collection can't start or end with '.': " ++ show col
    False -> return ()
  let cmd = ("create", toBson col') : List.map colCreateOptToBson opts
  _ <- dbCmd c db $ toBsonDoc cmd
  return ()

-- | Drop a collection.
dropCollection :: Connection -> FullCollection -> IO ()
dropCollection c col = do
  let (db, col') = splitFullCol col
  _ <- dbCmd c db $ toBsonDoc [("drop", toBson col')]
  return ()

-- | Return a string of validation info about the collection.
--
-- Example output (note this probably can/will change with different
-- versions of the server):
--
-- > validate
-- >  details: 0x7fe5cc2c1da4 ofs:e7da4
-- >  firstExtent:0:24100 ns:test.foo.bar
-- >  lastExtent:0:24100 ns:test.foo.bar
-- >  # extents:1
-- >  datasize?:180 nrecords?:5 lastExtentSize:1024
-- >  padding:1
-- >  first extent:
-- >    loc:0:24100 xnext:null xprev:null
-- >    nsdiag:test.foo.bar
-- >    size:1024 firstRecord:0:241e4 lastRecord:0:24280
-- >  5 objects found, nobj:5
-- >  260 bytes data w/headers
-- >  180 bytes data wout/headers
-- >  deletedList: 0100100000000000000
-- >  deleted: n: 4 size: 588
-- >  nIndexes:1
-- >    test.foo.bar.$_id_ keys:5
validateCollection :: Connection -> FullCollection -> IO String
validateCollection c col = do
  let (db, col') = splitFullCol col
  res <- dbCmd c db $ toBsonDoc [("validate", toBson col')]
  return $ fromBson $ fromJust $ BSON.lookup "result" res

splitFullCol :: FullCollection -> (Database, Collection)
splitFullCol col = (List.takeWhile (/= '.') col,
                    List.tail $ List.dropWhile (/= '.') col)

dbCmd :: Connection -> Database -> BsonDoc -> IO BsonDoc
dbCmd c db cmd = do
  mres <- findOne c (db ++ ".$cmd") cmd
  let res = fromJust mres
  case fromBson $ fromJust $ BSON.lookup "ok" res :: Int of
    1 -> return ()
    _ -> throwOpFailure $ "command \"" ++ show cmd ++ "\" failed: " ++
         (fromBson $ fromJust $ BSON.lookup "errmsg" res)
  return res

-- | An Itertaor over the results of a query. Use 'nextDoc' to get each
-- successive result document, or 'allDocs' or 'allDocs'' to get lazy or
-- strict lists of results.
data Cursor = Cursor {
      curCon :: Connection,
      curID :: IORef Int64,
      curNumToRet :: Int32,
      curCol :: FullCollection,
      curDocBytes :: IORef L.ByteString,
      curClosed :: IORef Bool
    }

data Opcode
    = OP_REPLY          -- 1     Reply to a client request. responseTo is set
    | OP_MSG            -- 1000	 generic msg command followed by a string
    | OP_UPDATE         -- 2001  update document
    | OP_INSERT	        -- 2002	 insert new document
    | OP_GET_BY_OID	-- 2003	 is this used?
    | OP_QUERY	        -- 2004	 query a collection
    | OP_GET_MORE	-- 2005	 Get more data from a query. See Cursors
    | OP_DELETE	        -- 2006	 Delete documents
    | OP_KILL_CURSORS	-- 2007	 Tell database client is done with a cursor
    deriving (Show, Eq)

data MongoDBInternalError = MongoDBInternalError String
                            deriving (Eq, Show, Read)

mongoDBInternalError :: TyCon
mongoDBInternalError = mkTyCon "Database.MongoDB.MongoDBInternalError"

instance Typeable MongoDBInternalError where
    typeOf _ = mkTyConApp mongoDBInternalError []

instance Exception MongoDBInternalError

data MongoDBCollectionInvalid = MongoDBCollectionInvalid String
                                deriving (Eq, Show, Read)

mongoDBCollectionInvalid :: TyCon
mongoDBCollectionInvalid = mkTyCon "Database.MongoDB.MongoDBcollectionInvalid"

instance Typeable MongoDBCollectionInvalid where
    typeOf _ = mkTyConApp mongoDBCollectionInvalid []

instance Exception MongoDBCollectionInvalid

throwColInvalid :: String -> a
throwColInvalid s = throw $ MongoDBCollectionInvalid s

data MongoDBOperationFailure = MongoDBOperationFailure String
                                deriving (Eq, Show, Read)

mongoDBOperationFailure :: TyCon
mongoDBOperationFailure = mkTyCon "Database.MongoDB.MongoDBoperationFailure"

instance Typeable MongoDBOperationFailure where
    typeOf _ = mkTyConApp mongoDBOperationFailure []

instance Exception MongoDBOperationFailure

throwOpFailure :: String -> a
throwOpFailure s = throw $ MongoDBOperationFailure s

fromOpcode :: Opcode -> Int32
fromOpcode OP_REPLY        =    1
fromOpcode OP_MSG          = 1000
fromOpcode OP_UPDATE       = 2001
fromOpcode OP_INSERT       = 2002
fromOpcode OP_GET_BY_OID   = 2003
fromOpcode OP_QUERY        = 2004
fromOpcode OP_GET_MORE     = 2005
fromOpcode OP_DELETE       = 2006
fromOpcode OP_KILL_CURSORS = 2007

toOpcode :: Int32 -> Opcode
toOpcode    1 = OP_REPLY
toOpcode 1000 = OP_MSG
toOpcode 2001 = OP_UPDATE
toOpcode 2002 = OP_INSERT
toOpcode 2003 = OP_GET_BY_OID
toOpcode 2004 = OP_QUERY
toOpcode 2005 = OP_GET_MORE
toOpcode 2006 = OP_DELETE
toOpcode 2007 = OP_KILL_CURSORS
toOpcode n = throw $ MongoDBInternalError $ "Got unexpected Opcode: " ++ show n

-- | The name of a database.
type Database = String

-- | The full collection name. The full collection name is the
-- concatenation of the database name with the collection name, using
-- a @.@ for the concatenation. For example, for the database @foo@
-- and the collection @bar@, the full collection name is @foo.bar@.
type FullCollection = String

-- | The same as 'FullCollection' but without the 'Database' prefix.
type Collection = String

-- | A 'BsonDoc' representing restrictions for a query much like the
-- /where/ part of an SQL query.
type Selector = BsonDoc
-- | A list of field names that limits the fields in the returned
-- documents. The list can contains zero or more elements, each of
-- which is the name of a field that should be returned. An empty list
-- means that no limiting is done and all fields are returned.
type FieldSelector = [L8.ByteString]
type RequestID = Int32
-- | Sets the number of documents to omit - starting from the first
-- document in the resulting dataset - when returning the result of
-- the query.
type NumToSkip = Int32
-- | This controls how many documents are returned at a time. The
-- cursor works by requesting /NumToReturn/ documents, which are then
-- immediately all transfered over the network; these are held locally
-- until the those /NumToReturn/ are all consumed and then the network
-- will be hit again for the next /NumToReturn/ documents.
--
-- If the value @0@ is given, the database will choose the number of
-- documents to return.
--
-- Otherwise choosing a good value is very dependant on the document size
-- and the way the cursor is being used.
type NumToReturn = Int32

-- | Options that control the behavior of a 'query' operation.
data QueryOpt = QO_TailableCursor
               | QO_SlaveOK
               | QO_OpLogReplay
               | QO_NoCursorTimeout
               deriving (Show)

fromQueryOpts :: [QueryOpt] -> Int32
fromQueryOpts opts = List.foldl (.|.) 0 $ fmap toVal opts
    where toVal QO_TailableCursor = 2
          toVal QO_SlaveOK = 4
          toVal QO_OpLogReplay = 8
          toVal QO_NoCursorTimeout = 16

-- | Options that effect the behavior of a 'update' operation.
data UpdateFlag = UF_Upsert
                | UF_Multiupdate
                deriving (Show, Enum)

fromUpdateFlags :: [UpdateFlag] -> Int32
fromUpdateFlags flags = List.foldl (.|.) 0 $
                        flip fmap flags $ (1 `shiftL`) . fromEnum

-- | Return the number of documents in /FullCollection/.
count :: Connection -> FullCollection -> IO Int64
count c col = countMatching c col BSON.empty

-- | Return the number of documents in /FullCollection/ matching /Selector/
countMatching :: Connection -> FullCollection -> Selector -> IO Int64
countMatching c col sel = do
  let (db, col') = splitFullCol col
  res <- dbCmd c db $ toBsonDoc [("count", toBson col'),
                                 ("query", BsonObject sel)]
  return $ fromBson $ fromJust $ BSON.lookup "n" res

-- | Delete documents matching /Selector/ from the given /FullCollection/.
delete :: Connection -> FullCollection -> Selector -> IO RequestID
delete c col sel = do
  let body = runPut $ do
                     putI32 0
                     putCol col
                     putI32 0
                     put sel
  (reqID, msg) <- packMsg c OP_DELETE body
  L.hPut (cHandle c) msg
  return reqID

-- | An alias for 'delete'.
remove :: Connection -> FullCollection -> Selector -> IO RequestID
remove = delete

-- | Insert a single document into /FullCollection/.
insert :: Connection -> FullCollection -> BsonDoc -> IO RequestID
insert c col doc = do
  let body = runPut $ do
                     putI32 0
                     putCol col
                     put doc
  (reqID, msg) <- packMsg c OP_INSERT body
  L.hPut (cHandle c) msg
  return reqID

-- | Insert a list of documents into /FullCollection/.
insertMany :: Connection -> FullCollection -> [BsonDoc] -> IO RequestID
insertMany c col docs = do
  let body = runPut $ do
               putI32 0
               putCol col
               forM_ docs put
  (reqID, msg) <- packMsg c OP_INSERT body
  L.hPut (cHandle c) msg
  return reqID

-- | Open a cursor to find documents. If you need full functionality,
-- see 'query'
find :: Connection -> FullCollection -> Selector -> IO Cursor
find c col sel = query c col [] 0 0 sel []

-- | Query, but only return the first result, if any.
findOne :: Connection -> FullCollection -> Selector -> IO (Maybe BsonDoc)
findOne c col sel = do
  cur <- query c col [] 0 (-1) sel []
  el <- nextDoc cur
  finish cur
  return el

-- | Perform a query and return the result as a lazy list. Be sure to
-- understand the comments about using the lazy list given for
-- 'allDocs'.
quickFind :: Connection -> FullCollection -> Selector -> IO [BsonDoc]
quickFind c col sel = find c col sel >>= allDocs

-- | Perform a query and return the result as a strict list.
quickFind' :: Connection -> FullCollection -> Selector -> IO [BsonDoc]
quickFind' c col sel = find c col sel >>= allDocs'

-- | Open a cursor to find documents in /FullCollection/ that match
-- /Selector/. See the documentation for each argument's type for
-- information about how it effects the query.
query :: Connection -> FullCollection -> [QueryOpt] ->
         NumToSkip -> NumToReturn -> Selector -> FieldSelector -> IO Cursor
query c col opts nskip ret sel fsel = do
  let h = cHandle c

  let body = runPut $ do
               putI32 $ fromQueryOpts opts
               putCol col
               putI32 nskip
               putI32 ret
               put sel
               case fsel of
                    [] -> putNothing
                    _ -> put $ toBsonDoc $ List.zip fsel $ repeat $ BsonInt32 1
  (reqID, msg) <- packMsg c OP_QUERY body
  L.hPut h msg

  hdr <- getHeader h
  assert (OP_REPLY == hOp hdr) $ return ()
  assert (hRespTo hdr == reqID) $ return ()
  reply <- getReply h
  assert (rRespFlags reply == 0) $ return ()
  docBytes <- (L.hGet h $ fromIntegral $ hMsgLen hdr - 16 - 20) >>= newIORef
  closed <- newIORef False
  cid <- newIORef $ rCursorID reply
  return $ Cursor {
               curCon = c,
               curID = cid,
               curNumToRet = ret,
               curCol = col,
               curDocBytes = docBytes,
               curClosed = closed
             }

-- | Update documents with /BsonDoc/ in /FullCollection/ that match /Selector/.
update :: Connection -> FullCollection ->
          [UpdateFlag] -> Selector -> BsonDoc -> IO RequestID
update c col flags sel obj = do
  let body = runPut $ do
               putI32 0
               putCol col
               putI32 $ fromUpdateFlags flags
               put sel
               put obj
  (reqID, msg) <- packMsg c OP_UPDATE body
  L.hPut (cHandle c) msg
  return reqID

data Hdr = Hdr {
      hMsgLen :: Int32,
      -- hReqID :: Int32,
      hRespTo :: Int32,
      hOp :: Opcode
    } deriving (Show)

data Reply = Reply {
      rRespFlags :: Int32,
      rCursorID :: Int64
      -- rStartFrom :: Int32,
      -- rNumReturned :: Int32
    } deriving (Show)

getHeader :: Handle -> IO Hdr
getHeader h = do
  hdrBytes <- L.hGet h 16
  return $ flip runGet hdrBytes $ do
                msgLen <- getI32
                skip 4 -- reqID <- getI32
                respTo <- getI32
                op <- getI32
                return $ Hdr msgLen respTo $ toOpcode op

getReply :: Handle -> IO Reply
getReply h = do
  replyBytes <- L.hGet h 20
  return $ flip runGet replyBytes $ do
               respFlags <- getI32
               cursorID <- getI64
               skip 4 -- startFrom <- getI32
               skip 4 -- numReturned <- getI32
               return $ (Reply respFlags cursorID)


-- | Return one document or Nothing if there are no more.
-- Automatically closes the curosr when last document is read
nextDoc :: Cursor -> IO (Maybe BsonDoc)
nextDoc cur = do
  closed <- readIORef $ curClosed cur
  case closed of
    True -> return Nothing
    False -> do
      docBytes <- readIORef $ curDocBytes cur
      cid <- readIORef $ curID cur
      case L.length docBytes of
        0 -> if cid == 0
             then writeIORef (curClosed cur) True >> return Nothing
             else getMore cur
        _ -> do
           let (doc, docBytes') = getFirstDoc docBytes
           writeIORef (curDocBytes cur) docBytes'
           return $ Just doc

-- | Return a lazy list of all (of the rest) of the documents in the
-- cursor. This works much like hGetContents--it will lazily read the
-- cursor data out of the database as the list is used. The cursor is
-- automatically closed when the list has been fully read.
--
-- If you manually finish the cursor before consuming off this list
-- you won't get all the original documents in the cursor.
--
-- If you don't consume to the end of the list, you must manually
-- close the cursor or you will leak the cursor, which may also leak
-- on the database side.
allDocs :: Cursor -> IO [BsonDoc]
allDocs cur = unsafeInterleaveIO $ do
                doc <- nextDoc cur
                case doc of
                  Nothing -> return []
                  Just d -> allDocs cur >>= return . (d :)

-- | Returns a strict list of all (of the rest) of the documents in
-- the cursor. This means that all of the documents will immediately
-- be read out of the database and loaded into memory.
allDocs' :: Cursor -> IO [BsonDoc]
allDocs' cur = do
  doc <- nextDoc cur
  case doc of
    Nothing -> return []
    Just d -> allDocs' cur >>= return . (d :)

getFirstDoc :: L.ByteString -> (BsonDoc, L.ByteString)
getFirstDoc docBytes = flip runGet docBytes $ do
                         doc <- get
                         docBytes' <- getRemainingLazyByteString
                         return (doc, docBytes')

getMore :: Cursor -> IO (Maybe BsonDoc)
getMore cur = do
  let h = cHandle $ curCon cur

  cid <- readIORef $ curID cur
  let body = runPut $ do
                putI32 0
                putCol $ curCol cur
                putI32 $ curNumToRet cur
                putI64 cid
  (reqID, msg) <- packMsg (curCon cur) OP_GET_MORE body
  L.hPut h msg

  hdr <- getHeader h
  assert (OP_REPLY == hOp hdr) $ return ()
  assert (hRespTo hdr == reqID) $ return ()
  reply <- getReply h
  assert (rRespFlags reply == 0) $ return ()
  case rCursorID reply of
       0 -> writeIORef (curID cur) 0
       ncid -> assert (ncid == cid) $ return ()
  docBytes <- (L.hGet h $ fromIntegral $ hMsgLen hdr - 16 - 20)
  case L.length docBytes of
    0 -> writeIORef (curClosed cur) True >> return Nothing
    _ -> do
      let (doc, docBytes') = getFirstDoc docBytes
      writeIORef (curDocBytes cur) docBytes'
      return $ Just doc

-- | Manually close a cursor -- usually not needed if you use
-- 'allDocs', 'allDocs'', or 'nextDoc'.
finish :: Cursor -> IO ()
finish cur = do
  let h = cHandle $ curCon cur
  cid <- readIORef $ curID cur
  let body = runPut $ do
                 putI32 0
                 putI32 1
                 putI64 cid
  (_reqID, msg) <- packMsg (curCon cur) OP_KILL_CURSORS body
  L.hPut h msg
  writeIORef (curClosed cur) True
  return ()

putCol :: Collection -> Put
putCol col = putByteString (pack col) >> putNull

packMsg :: Connection -> Opcode -> L.ByteString -> IO (RequestID, L.ByteString)
packMsg c op body = do
  reqID <- randNum c
  let msg = runPut $ do
                      putI32 $ fromIntegral $ L.length body + 16
                      putI32 reqID
                      putI32 0
                      putI32 $ fromOpcode op
                      putLazyByteString body
  return (reqID, msg)

randNum :: Connection -> IO Int32
randNum Connection { cRand = nsRef } = atomicModifyIORef nsRef $ \ns ->
                                       (List.tail ns,
                                        fromIntegral $ List.head ns)
