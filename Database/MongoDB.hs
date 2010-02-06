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
     connect, connectOnPort, conClose, disconnect, dropDatabase,
     connectCluster, connectClusterOnPort, setTarget,
     serverInfo, serverShutdown,
     databasesInfo, databaseNames,
     -- * Database
     Database, MongoDBCollectionInvalid, Password, Username,
     ColCreateOpt(..),
     collectionNames, createCollection, dropCollection,
     renameCollection, runCommand, validateCollection,
     login, addUser,
     -- * Collection
     Collection, FieldSelector, FullCollection,
     NumToSkip, NumToReturn, Selector,
     QueryOpt(..),
     UpdateFlag(..),
     count, countMatching, delete, insert, insertMany, query, remove, update,
     save,
     -- * Convenience collection operations
     find, findOne, quickFind, quickFind',
     -- * Query Helpers
     whereClause,
     -- * Cursor
     Cursor,
     allDocs, allDocs', finish, nextDoc,
     -- * Index
     Key, Unique,
     Direction(..),
     createIndex, dropIndex, dropIndexes, indexInformation,
    )
where
import Control.Exception
import Control.Monad
import Data.Binary()
import Data.Binary.Get
import Data.Binary.Put
import Data.Bits
import Data.ByteString.Char8 (pack)
import Data.ByteString.Internal (c2w)
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy.UTF8 as L8
import Data.Int
import Data.IORef
import qualified Data.List as List
import qualified Data.Map as Map
import Data.Maybe
import Data.Typeable
import Data.Digest.OpenSSL.MD5
import Database.MongoDB.BSON as BSON
import Database.MongoDB.Util
import qualified Network
import Network.Socket hiding (connect, send, sendTo, recv, recvFrom)
import Prelude hiding (getContents)
import System.IO
import System.IO.Unsafe
import System.Random

-- | A list of handles to database connections
data Connection = Connection { cHandles :: [Handle]
                              ,cIndex :: IORef Int
                              ,cRand :: IORef [Int] }

-- | Establish a connection to a MongoDB server
connect :: HostName -> IO Connection
connect = flip connectOnPort $ Network.PortNumber 27017

-- | Establish connections to a list of MongoDB servers
connectCluster :: [HostName] -> IO Connection
connectCluster xs =
    connectClusterOnPort $ fmap (flip (,) $ Network.PortNumber 27017) xs

-- | Establish connections to a list of MongoDB servers specifying each port.
connectClusterOnPort :: [(HostName, Network.PortID)] -> IO Connection
connectClusterOnPort [] = throwOpFailure "No hostnames in list"
connectClusterOnPort xs = newConnection >>= flip connectAll xs

connectAll :: Connection -> [(HostName, Network.PortID)] -> IO Connection
connectAll c [] = return c
connectAll c ((host, port) : xs) = do
  h <- Network.connectTo host port
  hSetBuffering h NoBuffering
  connectAll (c {cHandles = h : cHandles c}) xs

-- | Establish a connection to a MongoDB server on a non-standard port
connectOnPort :: HostName -> Network.PortID -> IO Connection
connectOnPort host port = do
  c <- newConnection
  connectAll c [(host, port)]

newConnection :: IO Connection
newConnection = do
  r <- newStdGen
  let ns = randomRs (fromIntegral (minBound :: Int32),
                     fromIntegral (maxBound :: Int32)) r
  nsRef <- newIORef ns
  nsIdx <- newIORef 0
  return $ Connection [] nsIdx nsRef

getHandle :: Connection -> IO Handle
getHandle c = do
  i <- readIORef $ cIndex c
  return $ cHandles c !! i

cPut :: Connection -> L.ByteString -> IO ()
cPut c msg = getHandle c >>= flip L.hPut msg

-- | Close database connection
conClose :: Connection -> IO ()
conClose c = mapM_ hClose $ cHandles c

setTarget :: Connection -> Int -> IO ()
setTarget c i =
  if i > length (cHandles c)
    then throwOpFailure "Target index higher than length of list"
    else writeIORef (cIndex c) i >> return ()

-- | Information about the databases on the server.
databasesInfo :: Connection -> IO BsonDoc
databasesInfo c =
    runCommand c (s2L "admin") $ toBsonDoc [("listDatabases", toBson (1::Int))]

-- | Return a list of database names on the server.
databaseNames :: Connection -> IO [Database]
databaseNames c = do
    info <- databasesInfo c
    let (BsonArray dbs) = fromLookup $ Map.lookup (s2L "databases") info
        names = mapMaybe (Map.lookup (s2L "name") . fromBson) dbs
    return $ List.map fromBson (names::[BsonValue])

-- | Alias for 'conClose'
disconnect :: Connection -> IO ()
disconnect = conClose

-- | Drop a database.
dropDatabase :: Connection -> Database -> IO ()
dropDatabase c db = do
  _ <- runCommand c db $ toBsonDoc [("dropDatabase", toBson (1::Int))]
  return ()

-- | Get information about the MongoDB server we're connected to.
serverInfo :: Connection -> IO BsonDoc
serverInfo c =
  runCommand c (s2L "admin") $ toBsonDoc [("buildinfo", toBson (1::Int))]

-- | Shut down the MongoDB server.
--
-- Force a clean exit, flushing and closing all data files.
-- Note that it will wait until all ongoing operations are complete.
serverShutdown :: Connection -> IO BsonDoc
serverShutdown c =
  runCommand c (s2L "admin") $ toBsonDoc [("shutdown", toBson (1::Int))]

-- | Return a list of collections in /Database/.
collectionNames :: Connection -> Database -> IO [FullCollection]
collectionNames c db = do
  docs <- quickFind' c (L.append db $ s2L ".system.namespaces") empty
  let names = flip List.map docs $ fromBson . fromLookup . BSON.lookup "name"
  return $ List.filter (L.notElem $ c2w '$') names

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
  (db, col') <- validateCollectionName col
  dbcols <- collectionNames c db
  when (col `List.elem` dbcols) $
       throwColInvalid $ "Collection already exists: " ++ show col
  let cmd = ("create", toBson col') : List.map colCreateOptToBson opts
  _ <- runCommand c db $ toBsonDoc cmd
  return ()

-- | Drop a collection.
dropCollection :: Connection -> FullCollection -> IO ()
dropCollection c col = do
  let (db, col') = splitFullCol col
  _ <- runCommand c db $ toBsonDoc [("drop", toBson col')]
  return ()

-- | Rename a collection--first /FullCollection/ argument is the
-- existing name, the second is the new name. At the moment this command
-- can also be used to move a collection between databases.
renameCollection :: Connection -> FullCollection -> FullCollection -> IO ()
renameCollection c col newName = do
  _ <- validateCollectionName col
  _ <- runCommand c (s2L "admin") $ toBsonDoc [("renameCollection", toBson col),
                                               ("to", toBson newName)]
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
  res <- runCommand c db $ toBsonDoc [("validate", toBson col')]
  return $ fromBson $ fromLookup $ BSON.lookup "result" res

splitFullCol :: FullCollection -> (Database, Collection)
splitFullCol col = (L.takeWhile (c2w '.' /=) col,
                    L.tail $ L.dropWhile (c2w '.' /=) col)

-- | Run a database command. Usually this is unneeded as driver wraps
-- all of the commands for you (eg 'createCollection',
-- 'dropCollection', etc).
runCommand :: Connection -> Database -> BsonDoc -> IO BsonDoc
runCommand c db cmd = do
  mres <- findOne c (L.append db $ s2L ".$cmd") cmd
  let res = fromLookup mres
  when (1 /= (fromBson $ fromLookup $ BSON.lookup "ok" res :: Int)) $
       throwOpFailure $ "command \"" ++ show cmd ++ "\" failed: " ++
                      fromBson (fromLookup $ BSON.lookup "errmsg" res)
  return res

-- | An Iterator over the results of a query. Use 'nextDoc' to get each
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
    = OPReply          -- 1    Reply to a client request. responseTo is set
    | OPMsg            -- 1000 generic msg command followed by a string
    | OPUpdate         -- 2001 update document
    | OPInsert         -- 2002 insert new document
    | OPGetByOid       -- 2003 is this used?
    | OPQuery	       -- 2004 query a collection
    | OPGetMore        -- 2005 Get more data from a query. See Cursors
    | OPDelete         -- 2006 Delete documents
    | OPKillCursors    -- 2007 Tell database client is done with a cursor
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
throwColInvalid = throw . MongoDBCollectionInvalid

data MongoDBOperationFailure = MongoDBOperationFailure String
                                deriving (Eq, Show, Read)

mongoDBOperationFailure :: TyCon
mongoDBOperationFailure = mkTyCon "Database.MongoDB.MongoDBoperationFailure"

instance Typeable MongoDBOperationFailure where
    typeOf _ = mkTyConApp mongoDBOperationFailure []

instance Exception MongoDBOperationFailure

throwOpFailure :: String -> a
throwOpFailure = throw . MongoDBOperationFailure

fromOpcode :: Opcode -> Int32
fromOpcode OPReply        =    1
fromOpcode OPMsg          = 1000
fromOpcode OPUpdate       = 2001
fromOpcode OPInsert       = 2002
fromOpcode OPGetByOid   = 2003
fromOpcode OPQuery        = 2004
fromOpcode OPGetMore      = 2005
fromOpcode OPDelete       = 2006
fromOpcode OPKillCursors  = 2007

toOpcode :: Int32 -> Opcode
toOpcode    1 = OPReply
toOpcode 1000 = OPMsg
toOpcode 2001 = OPUpdate
toOpcode 2002 = OPInsert
toOpcode 2003 = OPGetByOid
toOpcode 2004 = OPQuery
toOpcode 2005 = OPGetMore
toOpcode 2006 = OPDelete
toOpcode 2007 = OPKillCursors
toOpcode n = throw $ MongoDBInternalError $ "Got unexpected Opcode: " ++ show n

-- | The name of a database.
type Database = L8.ByteString

-- | The full collection name. The full collection name is the
-- concatenation of the database name with the collection name, using
-- a @.@ for the concatenation. For example, for the database @foo@
-- and the collection @bar@, the full collection name is @foo.bar@.
type FullCollection = L8.ByteString

-- | The same as 'FullCollection' but without the 'Database' prefix.
type Collection = L8.ByteString

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

type Username = String
type Password = String

-- | Options that control the behavior of a 'query' operation.
data QueryOpt = QOTailableCursor
              | QOSlaveOK
              | QOOpLogReplay
              | QONoCursorTimeout
                deriving (Show)

fromQueryOpts :: [QueryOpt] -> Int32
fromQueryOpts opts = List.foldl (.|.) 0 $ fmap toVal opts
    where toVal QOTailableCursor = 2
          toVal QOSlaveOK = 4
          toVal QOOpLogReplay = 8
          toVal QONoCursorTimeout = 16

-- | Options that effect the behavior of a 'update' operation.
data UpdateFlag = UFUpsert
                | UFMultiupdate
                deriving (Show, Enum)

fromUpdateFlags :: [UpdateFlag] -> Int32
fromUpdateFlags flags = List.foldl (.|.) 0 $
                        flip fmap flags $ (1 `shiftL`) . fromEnum

-- | Return the number of documents in /FullCollection/.
count :: Connection -> FullCollection -> IO Int64
count c col = countMatching c col empty

-- | Return the number of documents in /FullCollection/ matching /Selector/
countMatching :: Connection -> FullCollection -> Selector -> IO Int64
countMatching c col sel = do
  let (db, col') = splitFullCol col
  res <- runCommand c db $ toBsonDoc [("count", toBson col'),
                                      ("query", toBson sel)]
  return $ fromBson $ fromLookup $ BSON.lookup "n" res

-- | Delete documents matching /Selector/ from the given /FullCollection/.
delete :: Connection -> FullCollection -> Selector -> IO RequestID
delete c col sel = do
  let body = runPut $ do
                     putI32 0
                     putCol col
                     putI32 0
                     putBsonDoc sel
  (reqID, msg) <- packMsg c OPDelete body
  cPut c msg
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
                     putBsonDoc doc
  (reqID, msg) <- packMsg c OPInsert body
  cPut c msg
  return reqID

-- | Insert a list of documents into /FullCollection/.
insertMany :: Connection -> FullCollection -> [BsonDoc] -> IO RequestID
insertMany c col docs = do
  let body = runPut $ do
               putI32 0
               putCol col
               forM_ docs putBsonDoc
  (reqID, msg) <- packMsg c OPInsert body
  cPut c msg
  return reqID

-- | Open a cursor to find documents. If you need full functionality,
-- see 'query'
find :: Connection -> FullCollection -> Selector -> IO Cursor
find c col sel = query c col [] 0 0 sel []

-- | Query, but only return the first result, if any.
findOne :: Connection -> FullCollection -> Selector -> IO (Maybe BsonDoc)
findOne c col sel = do
  cur <- query c col [] 0 (-1) sel []
  nextDoc cur

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
  h <- getHandle c

  let body = runPut $ do
               putI32 $ fromQueryOpts opts
               putCol col
               putI32 nskip
               putI32 ret
               putBsonDoc sel
               case fsel of
                    [] -> putNothing
                    _ -> putBsonDoc $ toBsonDoc $ List.zip fsel $
                         repeat $ BsonInt32 1
  (reqID, msg) <- packMsg c OPQuery body
  L.hPut h msg

  hdr <- getHeader h
  assert (OPReply == hOp hdr) $ return ()
  assert (hRespTo hdr == reqID) $ return ()
  reply <- getReply h
  assert (rRespFlags reply == 0) $ return ()
  docBytes <- L.hGet h (fromIntegral $ hMsgLen hdr - 16 - 20) >>= newIORef
  closed <- newIORef False
  cid <- newIORef $ rCursorID reply
  return Cursor {
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
               putBsonDoc sel
               putBsonDoc obj
  (reqID, msg) <- packMsg c OPUpdate body
  cPut c msg
  return reqID

-- | log into the mongodb /Database/ attached to the /Connection/
login :: Connection -> Database -> Username -> Password -> IO BsonDoc
login c db user pass = do
  doc <- runCommand c db (toBsonDoc [("getnonce", toBson (1 :: Int))])
  let nonce = fromBson $ fromLookup $ BSON.lookup "nonce" doc :: String
      digest = md5sum $ pack $ nonce ++ user ++
                       md5sum (pack (user ++ ":mongo:" ++ pass))
      request = toBsonDoc [("authenticate", toBson (1 :: Int)),
                           ("user", toBson user),
                           ("nonce", toBson nonce),
                           ("key", toBson digest)]
      in runCommand c db request

-- | create a new user in the current /Database/
addUser :: Connection -> Database -> Username -> Password -> IO BsonDoc
addUser c db user pass = do
  let userDoc = toBsonDoc [(s2L "user", toBson user)]
      fdb = L.append db (s2L ".system.users")
  doc <- findOne c fdb userDoc
  let pwd = md5sum $ pack (user ++ ":mongo:" ++ pass)
      doc' = Map.insert (s2L "pwd") (toBson pwd) (fromMaybe userDoc doc)
  _ <- save c fdb doc'
  return doc'

-- | Conveniently stores the /BsonDoc/ to the /FullCollection/
-- if there is an _id present in the /BsonDoc/ then it already has
-- a place in the DB, so we update it using the _id, otherwise
-- we insert it
save :: Connection -> FullCollection -> BsonDoc -> IO RequestID
save c fc doc =
  case Map.lookup (s2L "_id") doc of
    Nothing -> insert c fc doc
    Just obj -> update c fc [UFUpsert] (toBsonDoc [("_id", obj)]) doc

-- | Use this in the place of the query portion of a select type query
-- This uses javascript and a scope supplied by a /BsonDoc/ to evaluate
-- documents in the database for retrieval.
--
-- Example:
--
-- > findOne conn mycoll $ whereClause "this.name == (name1 + name2)"
-- >     (toBsonDoc [("name1", toBson "mar"), ("name2", toBson "tha")])
whereClause :: String -> BsonDoc -> BsonDoc
whereClause qry scope = toBsonDoc [("$where", BsonCodeWScope (s2L qry) scope)]

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
               return $ Reply respFlags cursorID


-- | Return one document or Nothing if there are no more.
-- Automatically closes the cursor when last document is read
nextDoc :: Cursor -> IO (Maybe BsonDoc)
nextDoc cur = do
  closed <- readIORef $ curClosed cur
  if closed
    then return Nothing
    else do
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
                  Just d -> liftM (d :) (allDocs cur)

-- | Returns a strict list of all (of the rest) of the documents in
-- the cursor. This means that all of the documents will immediately
-- be read out of the database and loaded into memory.
allDocs' :: Cursor -> IO [BsonDoc]
allDocs' cur = do
  doc <- nextDoc cur
  case doc of
    Nothing -> return []
    Just d -> liftM (d :) (allDocs' cur)

getFirstDoc :: L.ByteString -> (BsonDoc, L.ByteString)
getFirstDoc docBytes = flip runGet docBytes $ do
                         doc <- getBsonDoc
                         docBytes' <- getRemainingLazyByteString
                         return (doc, docBytes')

getMore :: Cursor -> IO (Maybe BsonDoc)
getMore cur = do
  h <- getHandle $ curCon cur

  cid <- readIORef $ curID cur
  let body = runPut $ do
                putI32 0
                putCol $ curCol cur
                putI32 $ curNumToRet cur
                putI64 cid
  (reqID, msg) <- packMsg (curCon cur) OPGetMore body
  L.hPut h msg

  hdr <- getHeader h
  assert (OPReply == hOp hdr) $ return ()
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
  h <- getHandle $ curCon cur
  cid <- readIORef $ curID cur
  unless (cid == 0) $ do
      let body = runPut $ do
                   putI32 0
                   putI32 1
                   putI64 cid
      (_reqID, msg) <- packMsg (curCon cur) OPKillCursors body
      L.hPut h msg
      writeIORef (curClosed cur) True
  return ()

-- | The field key to index on.
type Key = L8.ByteString

-- | Direction to index.
data Direction = Ascending
               | Descending
                 deriving (Show, Eq)

fromDirection :: Direction -> Int
fromDirection Ascending  = 1
fromDirection Descending = - 1

-- | Should this index guarantee uniqueness?
type Unique = Bool

-- | Create a new index on /FullCollection/ on the list of /Key/ /
--   /Direction/ pairs.
createIndex :: Connection -> FullCollection ->
               [(Key, Direction)] -> Unique -> IO L8.ByteString
createIndex c col keys uniq = do
  let (db, _col') = splitFullCol col
      name = indexName keys
      keysDoc = flip fmap keys $
                \(k, d) -> (k, toBson $ fromDirection d :: BsonValue)
  _ <- insert c (L.append db $ s2L ".system.indexes") $
       toBsonDoc [("name",   toBson name),
                  ("ns",     toBson col),
                  ("key",    toBson keysDoc),
                  ("unique", toBson uniq)]
  return name

-- | Drop the specified index on the given /FullCollection/.
dropIndex :: Connection -> FullCollection -> [(Key, Direction)] -> IO ()
dropIndex c col keys = do
  let (db, col') = splitFullCol col
      name = indexName keys
  _ <- runCommand c db $ toBsonDoc [("deleteIndexes", toBson col'),
                                    ("index", toBson name)]
  return ()

-- | Drop all indexes on /FullCollection/.
dropIndexes :: Connection -> FullCollection -> IO ()
dropIndexes c col = do
  let (db, col') = splitFullCol col
  _ <- runCommand c db $ toBsonDoc [("deleteIndexes", toBson col'),
                                    ("index", toBson "*")]
  return ()

-- | Return a BsonDoc describing the existing indexes on /FullCollection/.
--
-- With the current server versions (1.2) this will return documents
-- such as:
--
-- > {"key": {"lastname": -1, "firstname": 1},
-- >  "name": "lastname_-1_firstname_1",
-- >  "ns": "mydb.people",
-- >  "unique": true}
--
-- Which is a single key that indexes on @lastname@ (descending) and
-- then @firstname@ (ascending) on the collection @people@ of the
-- database @mydb@ with a uniqueness requirement.
indexInformation :: Connection -> FullCollection -> IO [BsonDoc]
indexInformation c col = do
  let (db, _col') = splitFullCol col
  quickFind' c (L.append db $ s2L ".system.indexes") $
             toBsonDoc [("ns", toBson col)]

indexName :: [(Key, Direction)] -> L8.ByteString
indexName = L.intercalate (s2L "_") . List.map partName
    where partName (k, Ascending)  = L.append k $ s2L "_1"
          partName (k, Descending) = L.append k $ s2L "_-1"

putCol :: Collection -> Put
putCol col = putLazyByteString col >> putNull

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

s2L :: String -> L8.ByteString
s2L = L8.fromString

validateCollectionName :: FullCollection -> IO (Database, Collection)
validateCollectionName col = do
  let (db, col') = splitFullCol col
  when (s2L ".." `List.elem` L.group col) $
       throwColInvalid $ "Collection can't contain \"..\": " ++ show col
  when (c2w '$' `L.elem` col &&
        not (s2L "oplog.$mail" `L.isPrefixOf` col' ||
             s2L "$cmd" `L.isPrefixOf` col')) $
       throwColInvalid $ "Collection can't contain '$': " ++ show col
  when (L.head col == c2w '.' || L.last col == c2w '.') $
       throwColInvalid $ "Collection can't start or end with '.': " ++ show col
  return (db, col')

fromLookup :: Maybe a -> a
fromLookup (Just m) = m
fromLookup Nothing = throwColInvalid "cannot find key"
