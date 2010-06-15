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

-- | A driver for MongoDB
--
-- This module lets you connect to MongoDB, do inserts, queries,
-- updates, etc. Also has many convience functions inspired by HDBC
-- such as more easily converting between the BsonValue types and
-- native Haskell types.
--
-- * Tutorial for this driver:
--   <http://github.com/srp/mongoDB/blob/master/tutorial.md>
--
-- * Map/Reduce example for this driver:
--   <http://github.com/srp/mongoDB/blob/master/map-reduce-example.md>
--
-- * MongoDB:
--   <http://www.mongodb.org/>
--

module Database.MongoDB
    (
     -- * Connection
     Connection, ConnectOpt(..),
     connect, connectOnPort, conClose, disconnect, dropDatabase,
     connectCluster, connectClusterOnPort,
     serverInfo, serverShutdown,
     databasesInfo, databaseNames,
     -- * Database
     Database, MongoDBCollectionInvalid, Password, Username,
     ColCreateOpt(..),
     collectionNames, createCollection, dropCollection,
     renameCollection, runCommand, validateCollection,
     auth, addUser, login, logout,
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
     -- * Map-Reduce
     MapReduceOpt(..),
     mapReduce, mapReduceWScopes,
     runMapReduce, runMapReduceWScopes,
     mapReduceResults,
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
import Data.Digest.OpenSSL.MD5
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

-- | A list of handles to database connections
data Connection = Connection {
      cHandle :: IORef Handle,
      cRand :: IORef [Int],
      cOidGen :: ObjectIdGen
    }

data ConnectOpt
    = SlaveOK -- ^ It's fine to connect to the slave
    deriving (Show, Eq)

-- | Establish a connection to a MongoDB server
connect :: HostName -> [ConnectOpt] -> IO Connection
connect = flip connectOnPort (Network.PortNumber 27017)

-- | Establish connections to a list of MongoDB servers
connectCluster :: [HostName] -> [ConnectOpt] -> IO Connection
connectCluster xs =
    connectClusterOnPort (fmap (flip (,) $ Network.PortNumber 27017) xs)

-- | Establish connections to a list of MongoDB servers specifying each port.
connectClusterOnPort :: [(HostName, Network.PortID)] -> [ConnectOpt]
                     -> IO Connection
connectClusterOnPort [] _ = throwOpFailure "No hostnames in list"
connectClusterOnPort servers opts = newConnection servers opts

-- | Establish a connection to a MongoDB server on a non-standard port
connectOnPort :: HostName -> Network.PortID -> [ConnectOpt] -> IO Connection
connectOnPort host port = newConnection [(host, port)]

newConnection :: [(HostName, Network.PortID)] -> [ConnectOpt] -> IO Connection
newConnection servers opts = do
  r <- newStdGen
  let ns = randomRs (fromIntegral (minBound :: Int32),
                     fromIntegral (maxBound :: Int32)) r
  nsRef <- newIORef ns
  hRef <- openHandle (head servers) >>= newIORef
  oidGen <- mkObjectIdGen
  let c = Connection hRef nsRef oidGen
  res <- isMaster c
  if fromBson (fromLookup $ List.lookup (s2L "ismaster") res) == (1::Int) ||
     isJust (List.elemIndex SlaveOK opts)
    then return c
    else case List.lookup (s2L "remote") res of
           Nothing -> throwConFailure "Couldn't find master to connect to"
           Just server -> do
             hRef' <- openHandle (splitHostPort $ fromBson server) >>= newIORef
             return $ c {cHandle = hRef'}

openHandle :: (HostName, Network.PortID) -> IO Handle
openHandle (host, port) = do
  h <- Network.connectTo host port
  hSetBuffering h NoBuffering
  return h

getHandle :: Connection -> IO Handle
getHandle c = readIORef $ cHandle c

cPut :: Connection -> L.ByteString -> IO ()
cPut c msg = getHandle c >>= flip L.hPut msg

-- | Close database connection
conClose :: Connection -> IO ()
conClose c = readIORef (cHandle c) >>= hClose

-- | Information about the databases on the server.
databasesInfo :: Connection -> IO BsonDoc
databasesInfo c =
    runCommand c (s2L "admin") $ toBsonDoc [("listDatabases",  BsonInt32 1)]

-- | Return a list of database names on the server.
databaseNames :: Connection -> IO [Database]
databaseNames c = do
    info <- databasesInfo c
    let (BsonArray dbs) = fromLookup $ List.lookup (s2L "databases") info
        names = mapMaybe (List.lookup (s2L "name") . fromBson) dbs
    return $ List.map fromBson (names::[BsonValue])

-- | Alias for 'conClose'
disconnect :: Connection -> IO ()
disconnect = conClose

-- | Drop a database.
dropDatabase :: Connection -> Database -> IO ()
dropDatabase c db = do
  _ <- runCommand c db $ toBsonDoc [("dropDatabase", BsonInt32 1)]
  return ()

isMaster :: Connection -> IO BsonDoc
isMaster c = runCommand c (s2L "admin") $ toBsonDoc [("ismaster", BsonInt32 1)]

-- | Get information about the MongoDB server we're connected to.
serverInfo :: Connection -> IO BsonDoc
serverInfo c =
  runCommand c (s2L "admin") $ toBsonDoc [("buildinfo", BsonInt32 1)]

-- | Shut down the MongoDB server.
--
-- Force a clean exit, flushing and closing all data files.
-- Note that it will wait until all ongoing operations are complete.
serverShutdown :: Connection -> IO BsonDoc
serverShutdown c =
  runCommand c (s2L "admin") $ toBsonDoc [("shutdown", BsonInt32 1)]

-- | Return a list of collections in /Database/.
collectionNames :: Connection -> Database -> IO [FullCollection]
collectionNames c db = do
  docs <- quickFind' c (L.append db $ s2L ".system.namespaces") empty
  let names = flip List.map docs $
              fromBson . fromLookup . List.lookup (s2L "name")
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
  return $ fromBson $ fromLookup $ List.lookup (s2L "result") res

splitFullCol :: FullCollection -> (Database, Collection)
splitFullCol col = (L.takeWhile (c2w '.' /=) col,
                    L.tail $ L.dropWhile (c2w '.' /=) col)

splitHostPort :: String -> (HostName, Network.PortID)
splitHostPort hp = (host, port)
    where host = List.takeWhile (':' /=) hp
          port = case List.dropWhile (':' /=) hp of
                   "" -> Network.PortNumber 27017
                   pstr -> Network.Service $ List.tail pstr

-- | Run a database command. Usually this is unneeded as driver wraps
-- all of the commands for you (eg 'createCollection',
-- 'dropCollection', etc).
runCommand :: Connection -> Database -> BsonDoc -> IO BsonDoc
runCommand c db cmd = do
  mres <- findOne c (L.append db $ s2L ".$cmd") cmd
  let res = fromLookup mres
  when (BsonDouble 1.0 /= fromLookup (List.lookup (s2L "ok") res)) $
       throwOpFailure $ "command \"" ++ show cmd ++ "\" failed: " ++
                      fromBson (fromLookup $ List.lookup (s2L "errmsg") res)
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

data MongoDBConnectionFailure = MongoDBConnectionFailure String
                                deriving (Eq, Show, Read)

mongoDBConnectionFailure :: TyCon
mongoDBConnectionFailure = mkTyCon "Database.MongoDB.MongoDBconnectionFailure"

instance Typeable MongoDBConnectionFailure where
    typeOf _ = mkTyConApp mongoDBConnectionFailure []

instance Exception MongoDBConnectionFailure

throwConFailure :: String -> a
throwConFailure = throw . MongoDBConnectionFailure

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

type JSCode = L8.ByteString

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
count :: Connection -> FullCollection -> IO Integer
count c col = countMatching c col empty

-- | Return the number of documents in /FullCollection/ matching /Selector/
countMatching :: Connection -> FullCollection -> Selector -> IO Integer
countMatching c col sel = do
  let (db, col') = splitFullCol col
  res <- runCommand c db $ toBsonDoc [("count", toBson col'),
                                      ("query", toBson sel)]
  let cnt = (fromBson $ fromLookup $ List.lookup (s2L "n") res :: Double)
  return $ truncate cnt

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

moveOidToFrontOrGen :: Connection -> BsonDoc -> IO BsonDoc
moveOidToFrontOrGen c doc =
    case List.lookup (s2L "_id") doc of
      Nothing -> do
        oid <- genObjectId $ cOidGen c
        return $ (s2L "_id", oid) : doc
      Just oid -> do
        let keyEq = (\(k1, _) (k2, _) -> k1 == k2)
            delByKey = \k -> List.deleteBy keyEq (k, undefined)
        return $ (s2L "_id", oid) : delByKey (s2L "_id") doc

-- | Insert a single document into /FullCollection/ returning the /_id/ field.
insert :: Connection -> FullCollection -> BsonDoc -> IO BsonValue
insert c col doc = do
  doc' <- moveOidToFrontOrGen c doc
  let body = runPut $ do
                     putI32 0
                     putCol col
                     putBsonDoc doc'
  (_reqID, msg) <- packMsg c OPInsert body
  cPut c msg
  return $ snd $ head doc'

-- | Insert a list of documents into /FullCollection/ returing the
-- /_id/ field for each one in the same order as they were given.
insertMany :: Connection -> FullCollection -> [BsonDoc] -> IO [BsonValue]
insertMany c col docs = do
  docs' <- mapM (moveOidToFrontOrGen c) docs
  let body = runPut $ do
               putI32 0
               putCol col
               forM_ docs' putBsonDoc
  (_, msg) <- packMsg c OPInsert body
  cPut c msg
  return $ List.map (snd . head) docs'

-- | Open a cursor to find documents. If you need full functionality,
-- see 'query'
find :: Connection -> FullCollection -> Selector -> IO Cursor
find c col sel = query c col [] 0 0 sel []

-- | Query, but only return the first result, if any.
findOne :: Connection -> FullCollection -> Selector -> IO (Maybe BsonDoc)
findOne c col sel = query c col [] 0 (-1) sel [] >>= nextDoc

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
  let nonce = fromBson $ fromLookup $ List.lookup (s2L "nonce") doc :: String
      digest = md5sum $ pack $ nonce ++ user ++
                       md5sum (pack (user ++ ":mongo:" ++ pass))
      request = toBsonDoc [("authenticate", toBson (1 :: Int)),
                           ("user", toBson user),
                           ("nonce", toBson nonce),
                           ("key", toBson digest)]
      in runCommand c db request

auth :: Connection -> Database -> Username -> Password -> IO BsonDoc
auth = login

logout :: Connection -> Database -> IO ()
logout c db =
    runCommand c db (toBsonDoc [(s2L "logout", BsonInt32 1)]) >> return ()

-- | create a new user in the current /Database/
addUser :: Connection -> Database -> Username -> Password -> IO BsonDoc
addUser c db user pass = do
  let userDoc = toBsonDoc [(s2L "user", toBson user)]
      fdb = L.append db (s2L ".system.users")
  doc <- findOne c fdb userDoc
  let pwd = md5sum $ pack (user ++ ":mongo:" ++ pass)
      doc' = (s2L "pwd", toBson pwd) :
             List.deleteBy (\(k1,_) (k2,_) -> (k1 == k2))
                           (s2L user, undefined)
                           (fromMaybe userDoc doc)
  _ <- save c fdb doc'
  return doc'

data MapReduceOpt
    = MROptQuery BsonDoc      -- ^ query filter object

    -- | MRSort ???? TODO  <sort the query.  useful for optimization>

    | MROptLimit Int64        -- ^ number of objects to return from
                              -- collection

    | MROptOut L8.ByteString  -- ^ output-collection name

    | MROptKeepTemp           -- ^ If set the generated collection is
                              -- not treated as temporary, as it will
                              -- be by defualt. When /MROptOut/ is
                              -- specified, the collection is
                              -- automatically made permanent.

    | MROptFinalize JSCode    -- ^ function to apply to all the
                              -- results when finished

    | MROptScope BsonDoc      -- ^ can pass in variables that can be
                              -- access from map/reduce/finalize

    | MROptVerbose            -- ^ provide statistics on job execution
                              -- time

mrOptToTuple :: MapReduceOpt -> (String, BsonValue)
mrOptToTuple (MROptQuery q)    = ("query", BsonDoc q)
mrOptToTuple (MROptLimit l)    = ("limit", BsonInt64 l)
mrOptToTuple (MROptOut c)      = ("out", BsonString c)
mrOptToTuple MROptKeepTemp     = ("keeptemp", BsonBool True)
mrOptToTuple (MROptFinalize f) = ("finalize", BsonJSCode f)
mrOptToTuple (MROptScope s)    = ("scope", BsonDoc s)
mrOptToTuple MROptVerbose      = ("verbose", BsonBool True)

-- | Issue a map/reduce command and return the results metadata.  If
-- all you care about is the actual map/reduce results you might want
-- to use the 'mapReduce' command instead.
--
-- The results meta-document will look something like this:
--
-- > {"result": "tmp.mr.mapreduce_1268095152_14",
-- >  "timeMillis": 67,
-- >  "counts": {"input": 4,
-- >             "emit": 6,
-- >             "output": 3},
-- >  "ok": 1.0}
--
-- The /results/ field is the collection name within the same Database
-- that contain the results of the map/reduce.
runMapReduce :: Connection -> FullCollection
          -> JSCode -- ^ mapping javascript function
          -> JSCode -- ^ reducing javascript function
          -> [MapReduceOpt]
          -> IO BsonDoc
runMapReduce c fc m r opts = do
  let (db, col) = splitFullCol fc
      doc = [("mapreduce", toBson col),
             ("map", BsonJSCode m),
             ("reduce", BsonJSCode r)] ++ List.map mrOptToTuple opts
  runCommand c db $ toBsonDoc doc

-- | Issue a map/reduce command with associated scopes and return the
-- results metadata. If all you care about is the actual map/reduce
-- results you might want to use the 'mapReduce' command instead.
--
-- See 'runMapReduce' for more information about the form of the
-- result metadata.
runMapReduceWScopes :: Connection -> FullCollection
          -> JSCode -- ^ mapping javascript function
          -> BsonDoc -- ^ Scope for mapping function
          -> JSCode -- ^ reducing javascript function
          -> BsonDoc -- ^ Scope for reducing function
          -> [MapReduceOpt]
          -> IO BsonDoc
runMapReduceWScopes c fc m ms r rs opts = do
  let (db, col) = splitFullCol fc
      doc = [("mapreduce", toBson col),
             ("map", BsonJSCodeWScope m ms),
             ("reduce", BsonJSCodeWScope r rs)] ++ List.map mrOptToTuple opts
  runCommand c db $ toBsonDoc doc

-- | Given a result metadata from a 'mapReduce' command (or
-- 'mapReduceWScope'), issue the 'find' command that will produce the
-- actual map/reduce results.
mapReduceResults :: Connection -> Database -> BsonDoc -> IO Cursor
mapReduceResults c db r = do
  let col = case List.lookup (s2L "result") r of
              Just bCol -> fromBson bCol
              Nothing -> throwOpFailure "No 'result' in mapReduce response"
      fc = L.append (L.append db $ s2L ".") col
  find c fc []

-- | Run map/reduce and produce a cursor on the results.
mapReduce :: Connection -> FullCollection
          -> JSCode -- ^ mapping javascript function
          -> JSCode -- ^ reducing javascript function
          -> [MapReduceOpt]
          -> IO Cursor
mapReduce c fc m r opts =
    runMapReduce c fc m r opts >>= mapReduceResults c (fst $ splitFullCol fc)

-- | Run map/reduce with associated scopes and produce a cursor on the
-- results.
mapReduceWScopes :: Connection -> FullCollection
          -> JSCode -- ^ mapping javascript function
          -> BsonDoc -- ^ Scope for mapping function
          -> JSCode -- ^ reducing javascript function
          -> BsonDoc -- ^ Scope for mapping function
          -> [MapReduceOpt]
          -> IO Cursor
mapReduceWScopes c fc m ms r rs opts =
    runMapReduceWScopes c fc m ms r rs opts >>=
    mapReduceResults c (fst $ splitFullCol fc)

-- | Conveniently stores the /BsonDoc/ to the /FullCollection/
-- if there is an _id present in the /BsonDoc/ then it already has
-- a place in the DB, so we update it using the _id, otherwise
-- we insert it
save :: Connection -> FullCollection -> BsonDoc -> IO BsonValue
save c fc doc =
  case List.lookup (s2L "_id") doc of
    Nothing -> insert c fc doc
    Just oid -> update c fc [UFUpsert] (toBsonDoc [("_id", oid)]) doc >>
                return oid

-- | Use this in the place of the query portion of a select type query
-- This uses javascript and a scope supplied by a /BsonDoc/ to evaluate
-- documents in the database for retrieval.
--
-- Example:
--
-- > findOne conn mycoll $ whereClause "this.name == (name1 + name2)"
-- >     Just (toBsonDoc [("name1", toBson "mar"), ("name2", toBson "tha")])
whereClause :: String -> Maybe BsonDoc -> BsonDoc
whereClause qry Nothing = toBsonDoc [("$where", BsonJSCode (s2L qry))]
whereClause qry (Just scope) =
    toBsonDoc [("$where", BsonJSCodeWScope (s2L qry) scope)]

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
