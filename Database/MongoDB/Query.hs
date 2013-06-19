-- | Query and update documents

{-# LANGUAGE OverloadedStrings, RecordWildCards, NamedFieldPuns, TupleSections, FlexibleContexts, FlexibleInstances, UndecidableInstances, MultiParamTypeClasses, GeneralizedNewtypeDeriving, StandaloneDeriving, TypeSynonymInstances, TypeFamilies, CPP #-}

module Database.MongoDB.Query (
	-- * Monad
	Action, access, Failure(..), ErrorCode,
	AccessMode(..), GetLastError, master, slaveOk, accessMode, 
	MonadDB(..),
	-- * Database
	Database, allDatabases, useDb, thisDatabase,
	-- ** Authentication
	Username, Password, auth,
	-- * Collection
	Collection, allCollections,
	-- ** Selection
	Selection(..), Selector, whereJS,
	Select(select),
	-- * Write
	-- ** Insert
	insert, insert_, insertMany, insertMany_, insertAll, insertAll_,
	-- ** Update
	save, replace, repsert, Modifier, modify,
	-- ** Delete
	delete, deleteOne,
	-- * Read
	-- ** Query
	Query(..), QueryOption(NoCursorTimeout, TailableCursor, AwaitData, Partial),
    Projector, Limit, Order, BatchSize,
	explain, find, findOne, fetch, findAndModify, count, distinct,
	-- *** Cursor
	Cursor, nextBatch, next, nextN, rest, closeCursor, isCursorClosed,
	-- ** Aggregate
	Pipeline, aggregate,
	-- ** Group
	Group(..), GroupKey(..), group,
	-- ** MapReduce
	MapReduce(..), MapFun, ReduceFun, FinalizeFun, MROut(..), MRMerge(..),
    MRResult, mapReduce, runMR, runMR',
	-- * Command
	Command, runCommand, runCommand1,
	eval,
) where

import Prelude hiding (lookup)
import Control.Applicative (Applicative, (<$>))
import Control.Monad (unless, replicateM, liftM)
import Data.Int (Int32)
import Data.Maybe (listToMaybe, catMaybes)
import Data.Word (Word32)

#if MIN_VERSION_base(4,6,0)
import Control.Concurrent.MVar.Lifted (MVar, newMVar, mkWeakMVar,
                                       readMVar, modifyMVar)
#else
import Control.Concurrent.MVar.Lifted (MVar, newMVar, addMVarFinalizer,
                                         readMVar, modifyMVar)
#endif
import Control.Monad.Base (MonadBase(liftBase))
import Control.Monad.Error (ErrorT, Error(..), MonadError, runErrorT,
                            throwError)
import Control.Monad.Reader (ReaderT, runReaderT, ask, asks, local)
import Control.Monad.RWS (RWST)
import Control.Monad.State (StateT)
import Control.Monad.Trans (MonadIO, MonadTrans, lift, liftIO)
import Control.Monad.Trans.Control (ComposeSt, MonadBaseControl(..),
                                    MonadTransControl(..), StM, StT,
                                    defaultLiftBaseWith, defaultRestoreM)
import Control.Monad.Writer (WriterT, Monoid)
import Data.Bson (Document, Field(..), Label, Val, Value(String, Doc, Bool),
                  Javascript, at, valueAt, lookup, look, genObjectId, (=:),
                  (=?))
import Data.Text (Text)
import qualified Data.Text as T

import Database.MongoDB.Internal.Protocol (Reply(..), QueryOption(..),
                                           ResponseFlag(..), InsertOption(..),
                                           UpdateOption(..), DeleteOption(..),
                                           CursorId, FullCollection, Username,
                                           Password, Pipe, Notice(..),
                                           Request(GetMore, qOptions, qSkip,
                                           qFullCollection, qBatchSize,
                                           qSelector, qProjector),
                                           pwKey)
import Database.MongoDB.Internal.Util (MonadIO', loop, liftIOE, true1, (<.>))
import qualified Database.MongoDB.Internal.Protocol as P

#if !MIN_VERSION_base(4,6,0)
--mkWeakMVar = addMVarFinalizer
#endif

-- * Monad

newtype Action m a = Action {unAction :: ErrorT Failure (ReaderT Context m) a}
	deriving (Functor, Applicative, Monad, MonadIO, MonadError Failure)
-- ^ A monad on top of m (which must be a MonadIO) that may access the database and may fail with a DB 'Failure'

instance MonadBase b m => MonadBase b (Action m) where
     liftBase = Action . liftBase

instance (MonadIO m, MonadBaseControl b m) => MonadBaseControl b (Action m) where
     newtype StM (Action m) a = StMT {unStMT :: ComposeSt Action m a}
     liftBaseWith = defaultLiftBaseWith StMT
     restoreM     = defaultRestoreM   unStMT

instance MonadTrans Action where
     lift = Action . lift . lift

instance MonadTransControl Action where
    newtype StT Action a = StActionT {unStAction :: StT (ReaderT Context) (StT (ErrorT Failure) a)}
    liftWith f = Action $ liftWith $ \runError ->
                            liftWith $ \runReader' ->
                              f (liftM StActionT . runReader' . runError . unAction)
    restoreT = Action . restoreT . restoreT . liftM unStAction

access :: (MonadIO m) => Pipe -> AccessMode -> Database -> Action m a -> m (Either Failure a)
-- ^ Run action against database on server at other end of pipe. Use access mode for any reads and writes. Return Left on connection failure or read/write failure.
access myPipe myAccessMode myDatabase (Action action) = runReaderT (runErrorT action) Context{..}

-- | A connection failure, or a read or write exception like cursor expired or inserting a duplicate key.
-- Note, unexpected data from the server is not a Failure, rather it is a programming error (you should call 'error' in this case) because the client and server are incompatible and requires a programming change.
data Failure =
	 ConnectionFailure IOError  -- ^ TCP connection ('Pipeline') failed. May work if you try again on the same Mongo 'Connection' which will create a new Pipe.
	| CursorNotFoundFailure CursorId  -- ^ Cursor expired because it wasn't accessed for over 10 minutes, or this cursor came from a different server that the one you are currently connected to (perhaps a fail over happen between servers in a replica set)
	| QueryFailure ErrorCode String  -- ^ Query failed for some reason as described in the string
	| WriteFailure ErrorCode String  -- ^ Error observed by getLastError after a write, error description is in string
	| DocNotFound Selection  -- ^ 'fetch' found no document matching selection
	| AggregateFailure String -- ^ 'aggregate' returned an error
	deriving (Show, Eq)

type ErrorCode = Int
-- ^ Error code from getLastError or query failure

instance Error Failure where strMsg = error
-- ^ 'fail' is treated the same as a programming 'error'. In other words, don't use it.

-- | Type of reads and writes to perform
data AccessMode =
	 ReadStaleOk  -- ^ Read-only action, reading stale data from a slave is OK.
	| UnconfirmedWrites  -- ^ Read-write action, slave not OK, every write is fire & forget.
	| ConfirmWrites GetLastError  -- ^ Read-write action, slave not OK, every write is confirmed with getLastError.
    deriving Show

type GetLastError = Document
-- ^ Parameters for getLastError command. For example @[\"w\" =: 2]@ tells the server to wait for the write to reach at least two servers in replica set before acknowledging. See <http://www.mongodb.org/display/DOCS/Last+Error+Commands> for more options.

master :: AccessMode
-- ^ Same as 'ConfirmWrites' []
master = ConfirmWrites []

slaveOk :: AccessMode
-- ^ Same as 'ReadStaleOk'
slaveOk = ReadStaleOk

accessMode :: (Monad m) => AccessMode -> Action m a -> Action m a
-- ^ Run action with given 'AccessMode'
accessMode mode (Action act) = Action $ local (\ctx -> ctx {myAccessMode = mode}) act

readMode :: AccessMode -> ReadMode
readMode ReadStaleOk = StaleOk
readMode _ = Fresh

writeMode :: AccessMode -> WriteMode
writeMode ReadStaleOk = Confirm []
writeMode UnconfirmedWrites = NoConfirm
writeMode (ConfirmWrites z) = Confirm z

-- | Values needed when executing a db operation
data Context = Context {
	myPipe :: Pipe, -- ^ operations read/write to this pipelined TCP connection to a MongoDB server
	myAccessMode :: AccessMode, -- ^ read/write operation will use this access mode
	myDatabase :: Database } -- ^ operations query/update this database

myReadMode :: Context -> ReadMode
myReadMode = readMode . myAccessMode

myWriteMode :: Context -> WriteMode
myWriteMode = writeMode . myAccessMode

send :: (MonadIO m) => [Notice] -> Action m ()
-- ^ Send notices as a contiguous batch to server with no reply. Throw 'ConnectionFailure' if pipe fails.
send ns = Action $ do
	pipe <- asks myPipe
	liftIOE ConnectionFailure $ P.send pipe ns

call :: (MonadIO m) => [Notice] -> Request -> Action m (ErrorT Failure IO Reply)
-- ^ Send notices and request as a contiguous batch to server and return reply promise, which will block when invoked until reply arrives. This call will throw 'ConnectionFailure' if pipe fails on send, and promise will throw 'ConnectionFailure' if pipe fails on receive.
call ns r = Action $ do
	pipe <- asks myPipe
	promise <- liftIOE ConnectionFailure $ P.call pipe ns r
	return (liftIOE ConnectionFailure promise)

-- | If you stack a monad on top of 'Action' then make it an instance of this class and use 'liftDB' to execute a DB Action within it. Instances already exist for the basic mtl transformers.
class (Monad m, MonadBaseControl IO (BaseMonad m), Applicative (BaseMonad m), Functor (BaseMonad m)) => MonadDB m where
	type BaseMonad m :: * -> *
	liftDB :: Action (BaseMonad m) a -> m a

instance (MonadBaseControl IO m, Applicative m, Functor m) => MonadDB (Action m) where
	type BaseMonad (Action m) = m
	liftDB = id

instance (MonadDB m, Error e) => MonadDB (ErrorT e m) where
	type BaseMonad (ErrorT e m) = BaseMonad m
	liftDB = lift . liftDB
instance (MonadDB m) => MonadDB (ReaderT r m) where
	type BaseMonad (ReaderT r m) = BaseMonad m
	liftDB = lift . liftDB
instance (MonadDB m) => MonadDB (StateT s m) where
	type BaseMonad (StateT s m) = BaseMonad m
	liftDB = lift . liftDB
instance (MonadDB m, Monoid w) => MonadDB (WriterT w m) where
	type BaseMonad (WriterT w m) = BaseMonad m
	liftDB = lift . liftDB
instance (MonadDB m, Monoid w) => MonadDB (RWST r w s m) where
	type BaseMonad (RWST r w s m) = BaseMonad m
	liftDB = lift . liftDB

-- * Database

type Database = Text

allDatabases :: (MonadIO' m) => Action m [Database]
-- ^ List all databases residing on server
allDatabases = map (at "name") . at "databases" <$> useDb "admin" (runCommand1 "listDatabases")

thisDatabase :: (Monad m) => Action m Database
-- ^ Current database in use
thisDatabase = Action $ asks myDatabase

useDb :: (Monad m) => Database -> Action m a -> Action m a
-- ^ Run action against given database
useDb db (Action act) = Action $ local (\ctx -> ctx {myDatabase = db}) act

-- * Authentication

auth :: (MonadIO' m) => Username -> Password -> Action m Bool
-- ^ Authenticate with the current database (if server is running in secure mode). Return whether authentication was successful or not. Reauthentication is required for every new pipe.
auth usr pss = do
	n <- at "nonce" <$> runCommand ["getnonce" =: (1 :: Int)]
	true1 "ok" <$> runCommand ["authenticate" =: (1 :: Int), "user" =: usr, "nonce" =: n, "key" =: pwKey n usr pss]

-- * Collection

type Collection = Text
-- ^ Collection name (not prefixed with database)

allCollections :: (MonadIO m, MonadBaseControl IO m, Functor m) => Action m [Collection]
-- ^ List all collections in this database
allCollections = do
	db <- thisDatabase
	docs <- rest =<< find (query [] "system.namespaces") {sort = ["name" =: (1 :: Int)]}
	return . filter (not . isSpecial db) . map dropDbPrefix $ map (at "name") docs
 where
 	dropDbPrefix = T.tail . T.dropWhile (/= '.')
 	isSpecial db col = T.any (== '$') col && db <.> col /= "local.oplog.$main"

-- * Selection

data Selection = Select {selector :: Selector, coll :: Collection}  deriving (Show, Eq)
-- ^ Selects documents in collection that match selector

type Selector = Document
-- ^ Filter for a query, analogous to the where clause in SQL. @[]@ matches all documents in collection. @[\"x\" =: a, \"y\" =: b]@ is analogous to @where x = a and y = b@ in SQL. See <http://www.mongodb.org/display/DOCS/Querying> for full selector syntax.

whereJS :: Selector -> Javascript -> Selector
-- ^ Add Javascript predicate to selector, in which case a document must match both selector and predicate
whereJS sel js = ("$where" =: js) : sel

class Select aQueryOrSelection where
	select :: Selector -> Collection -> aQueryOrSelection
	-- ^ 'Query' or 'Selection' that selects documents in collection that match selector. The choice of type depends on use, for example, in @find (select sel col)@ it is a Query, and in @delete (select sel col)@ it is a Selection.

instance Select Selection where
	select = Select

instance Select Query where
	select = query

-- * Write

data WriteMode =
	  NoConfirm  -- ^ Submit writes without receiving acknowledgments. Fast. Assumes writes succeed even though they may not.
	| Confirm GetLastError  -- ^ Receive an acknowledgment after every write, and raise exception if one says the write failed. This is acomplished by sending the getLastError command, with given 'GetLastError' parameters, after every write.
	deriving (Show, Eq)

write :: (MonadIO m) => Notice -> Action m ()
-- ^ Send write to server, and if write-mode is 'Safe' then include getLastError request and raise 'WriteFailure' if it reports an error.
write notice = Action (asks myWriteMode) >>= \mode -> case mode of
	NoConfirm -> send [notice]
	Confirm params -> do
		let q = query (("getlasterror" =: (1 :: Int)) : params) "$cmd"
		Batch _ _ [doc] <- fulfill =<< request [notice] =<< queryRequest False q {limit = 1}
		case lookup "err" doc of
			Nothing -> return ()
			Just err -> throwError $ WriteFailure (maybe 0 id $ lookup "code" doc) err

-- ** Insert

insert :: (MonadIO' m) => Collection -> Document -> Action m Value
-- ^ Insert document into collection and return its \"_id\" value, which is created automatically if not supplied
insert col doc = head <$> insertMany col [doc]

insert_ :: (MonadIO' m) => Collection -> Document -> Action m ()
-- ^ Same as 'insert' except don't return _id
insert_ col doc = insert col doc >> return ()

insertMany :: (MonadIO m) => Collection -> [Document] -> Action m [Value]
-- ^ Insert documents into collection and return their \"_id\" values, which are created automatically if not supplied. If a document fails to be inserted (eg. due to duplicate key) then remaining docs are aborted, and LastError is set.
insertMany = insert' []

insertMany_ :: (MonadIO m) => Collection -> [Document] -> Action m ()
-- ^ Same as 'insertMany' except don't return _ids
insertMany_ col docs = insertMany col docs >> return ()

insertAll :: (MonadIO m) => Collection -> [Document] -> Action m [Value]
-- ^ Insert documents into collection and return their \"_id\" values, which are created automatically if not supplied. If a document fails to be inserted (eg. due to duplicate key) then remaining docs are still inserted. LastError is set if any doc fails, not just last one.
insertAll = insert' [KeepGoing]

insertAll_ :: (MonadIO m) => Collection -> [Document] -> Action m ()
-- ^ Same as 'insertAll' except don't return _ids
insertAll_ col docs = insertAll col docs >> return ()

insert' :: (MonadIO m) => [InsertOption] -> Collection -> [Document] -> Action m [Value]
-- ^ Insert documents into collection and return their \"_id\" values, which are created automatically if not supplied
insert' opts col docs = do
	db <- thisDatabase
	docs' <- liftIO $ mapM assignId docs
	write (Insert (db <.> col) opts docs')
	return $ map (valueAt "_id") docs'

assignId :: Document -> IO Document
-- ^ Assign a unique value to _id field if missing
assignId doc = if any (("_id" ==) . label) doc
	then return doc
	else (\oid -> ("_id" =: oid) : doc) <$> genObjectId

-- ** Update 

save :: (MonadIO' m) => Collection -> Document -> Action m ()
-- ^ Save document to collection, meaning insert it if its new (has no \"_id\" field) or update it if its not new (has \"_id\" field)
save col doc = case look "_id" doc of
	Nothing -> insert_ col doc
	Just i -> repsert (Select ["_id" := i] col) doc

replace :: (MonadIO m) => Selection -> Document -> Action m ()
-- ^ Replace first document in selection with given document
replace = update []

repsert :: (MonadIO m) => Selection -> Document -> Action m ()
-- ^ Replace first document in selection with given document, or insert document if selection is empty
repsert = update [Upsert]

type Modifier = Document
-- ^ Update operations on fields in a document. See <http://www.mongodb.org/display/DOCS/Updating#Updating-ModifierOperations>

modify :: (MonadIO m) => Selection -> Modifier -> Action m ()
-- ^ Update all documents in selection using given modifier
modify = update [MultiUpdate]

update :: (MonadIO m) => [UpdateOption] -> Selection -> Document -> Action m ()
-- ^ Update first document in selection using updater document, unless 'MultiUpdate' option is supplied then update all documents in selection. If 'Upsert' option is supplied then treat updater as document and insert it if selection is empty.
update opts (Select sel col) up = do
	db <- thisDatabase
	write (Update (db <.> col) opts sel up)

-- ** Delete

delete :: (MonadIO m) => Selection -> Action m ()
-- ^ Delete all documents in selection
delete = delete' []

deleteOne :: (MonadIO m) => Selection -> Action m ()
-- ^ Delete first document in selection
deleteOne = delete' [SingleRemove]

delete' :: (MonadIO m) => [DeleteOption] -> Selection -> Action m ()
-- ^ Delete all documents in selection unless 'SingleRemove' option is given then only delete first document in selection
delete' opts (Select sel col) = do
	db <- thisDatabase
	write (Delete (db <.> col) opts sel)

-- * Read

data ReadMode =
	  Fresh  -- ^ read from master only
	| StaleOk  -- ^ read from slave ok
	deriving (Show, Eq)

readModeOption :: ReadMode -> [QueryOption]
readModeOption Fresh = []
readModeOption StaleOk = [SlaveOK]

-- ** Query

-- | Use 'select' to create a basic query with defaults, then modify if desired. For example, @(select sel col) {limit = 10}@
data Query = Query {
	options :: [QueryOption],  -- ^ Default = []
	selection :: Selection,
	project :: Projector,  -- ^ \[\] = all fields. Default = []
	skip :: Word32,  -- ^ Number of initial matching documents to skip. Default = 0
	limit :: Limit, -- ^ Maximum number of documents to return, 0 = no limit. Default = 0
	sort :: Order,  -- ^ Sort results by this order, [] = no sort. Default = []
	snapshot :: Bool,  -- ^ If true assures no duplicates are returned, or objects missed, which were present at both the start and end of the query's execution (even if the object were updated). If an object is new during the query, or deleted during the query, it may or may not be returned, even with snapshot mode. Note that short query responses (less than 1MB) are always effectively snapshotted. Default = False
	batchSize :: BatchSize,  -- ^ The number of document to return in each batch response from the server. 0 means use Mongo default. Default = 0
	hint :: Order  -- ^ Force MongoDB to use this index, [] = no hint. Default = []
	} deriving (Show, Eq)

type Projector = Document
-- ^ Fields to return, analogous to the select clause in SQL. @[]@ means return whole document (analogous to * in SQL). @[\"x\" =: 1, \"y\" =: 1]@ means return only @x@ and @y@ fields of each document. @[\"x\" =: 0]@ means return all fields except @x@.

type Limit = Word32
-- ^ Maximum number of documents to return, i.e. cursor will close after iterating over this number of documents. 0 means no limit.

type Order = Document
-- ^ Fields to sort by. Each one is associated with 1 or -1. Eg. @[\"x\" =: 1, \"y\" =: -1]@ means sort by @x@ ascending then @y@ descending

type BatchSize = Word32
-- ^ The number of document to return in each batch response from the server. 0 means use Mongo default.

query :: Selector -> Collection -> Query
-- ^ Selects documents in collection that match selector. It uses no query options, projects all fields, does not skip any documents, does not limit result size, uses default batch size, does not sort, does not hint, and does not snapshot.
query sel col = Query [] (Select sel col) [] 0 0 [] False 0 []

find :: (MonadIO m, MonadBaseControl IO m) => Query -> Action m Cursor
-- ^ Fetch documents satisfying query
find q@Query{selection, batchSize} = do
	db <- thisDatabase
	dBatch <- request [] =<< queryRequest False q
	newCursor db (coll selection) batchSize dBatch

findOne :: (MonadIO m) => Query -> Action m (Maybe Document)
-- ^ Fetch first document satisfying query or Nothing if none satisfy it
findOne q = do
	Batch _ _ docs <- fulfill =<< request [] =<< queryRequest False q {limit = 1}
	return (listToMaybe docs)

fetch :: (MonadIO m) => Query -> Action m Document
-- ^ Same as 'findOne' except throw 'DocNotFound' if none match
fetch q = findOne q >>= maybe (throwError $ DocNotFound $ selection q) return

-- | runs the findAndModify command.
-- Returns a single updated document (new option is set to true).
-- Currently this API does not allow setting the remove option
findAndModify :: (Applicative m, MonadIO m)
              => Query
              -> Document -- ^ updates
              -> Action m (Either String Document)
findAndModify (Query {
    selection = Select sel collection
  , project = project
  , sort = sort
  }) updates = do
  result <- runCommand [
     "findAndModify" := String collection
   , "new"    := Bool True -- return updated document, not original document
   , "query"  := Doc sel
   , "update" := Doc updates
   , "fields" := Doc project
   , "sort"   := Doc sort
   ]
  return $ case findErr result of
    Nothing -> case lookup "value" result of
      Nothing -> Left "findAndModify: no document found (value field was empty)"
      Just doc -> Right doc
    Just e -> Left e
    where
      findErr result = lookup "err" (at "lastErrorObject" result)

explain :: (MonadIO m) => Query -> Action m Document
-- ^ Return performance stats of query execution
explain q = do  -- same as findOne but with explain set to true
	Batch _ _ docs <- fulfill =<< request [] =<< queryRequest True q {limit = 1}
	return $ if null docs then error ("no explain: " ++ show q) else head docs

count :: (MonadIO' m) => Query -> Action m Int
-- ^ Fetch number of documents satisfying query (including effect of skip and/or limit if present)
count Query{selection = Select sel col, skip, limit} = at "n" <$> runCommand
	(["count" =: col, "query" =: sel, "skip" =: (fromIntegral skip :: Int32)]
		++ ("limit" =? if limit == 0 then Nothing else Just (fromIntegral limit :: Int32)))

distinct :: (MonadIO' m) => Label -> Selection -> Action m [Value]
-- ^ Fetch distinct values of field in selected documents
distinct k (Select sel col) = at "values" <$> runCommand ["distinct" =: col, "key" =: k, "query" =: sel]

queryRequest :: (Monad m) => Bool -> Query -> Action m (Request, Limit)
-- ^ Translate Query to Protocol.Query. If first arg is true then add special $explain attribute.
queryRequest isExplain Query{..} = do
	ctx <- Action ask
	return $ queryRequest' (myReadMode ctx) (myDatabase ctx)
 where
	queryRequest' rm db = (P.Query{..}, remainingLimit) where
		qOptions = readModeOption rm ++ options
		qFullCollection = db <.> coll selection
		qSkip = fromIntegral skip
		(qBatchSize, remainingLimit) = batchSizeRemainingLimit batchSize limit
		qProjector = project
		mOrder = if null sort then Nothing else Just ("$orderby" =: sort)
		mSnapshot = if snapshot then Just ("$snapshot" =: True) else Nothing
		mHint = if null hint then Nothing else Just ("$hint" =: hint)
		mExplain = if isExplain then Just ("$explain" =: True) else Nothing
		special = catMaybes [mOrder, mSnapshot, mHint, mExplain]
		qSelector = if null special then s else ("$query" =: s) : special where s = selector selection

batchSizeRemainingLimit :: BatchSize -> Limit -> (Int32, Limit)
-- ^ Given batchSize and limit return P.qBatchSize and remaining limit
batchSizeRemainingLimit batchSize limit = if limit == 0
	then (fromIntegral batchSize', 0)  -- no limit
	else if 0 < batchSize' && batchSize' < limit
		then (fromIntegral batchSize', limit - batchSize')
		else (- fromIntegral limit, 1)
 where batchSize' = if batchSize == 1 then 2 else batchSize
 	-- batchSize 1 is broken because server converts 1 to -1 meaning limit 1

type DelayedBatch = ErrorT Failure IO Batch
-- ^ A promised batch which may fail

data Batch = Batch Limit CursorId [Document]
-- ^ CursorId = 0 means cursor is finished. Documents is remaining documents to serve in current batch. Limit is remaining limit for next fetch.

request :: (MonadIO m) => [Notice] -> (Request, Limit) -> Action m DelayedBatch
-- ^ Send notices and request and return promised batch
request ns (req, remainingLimit) = do
	promise <- call ns req
	return $ fromReply remainingLimit =<< promise

fromReply :: Limit -> Reply -> DelayedBatch
-- ^ Convert Reply to Batch or Failure
fromReply limit Reply{..} = do
	mapM_ checkResponseFlag rResponseFlags
	return (Batch limit rCursorId rDocuments)
 where
	-- If response flag indicates failure then throw it, otherwise do nothing
	checkResponseFlag flag = case flag of
		AwaitCapable -> return ()
		CursorNotFound -> throwError $ CursorNotFoundFailure rCursorId
		QueryError -> throwError $ QueryFailure (at "code" $ head rDocuments) (at "$err" $ head rDocuments)

fulfill :: (MonadIO m) => DelayedBatch -> Action m Batch
-- ^ Demand and wait for result, raise failure if exception
fulfill = Action . liftIOE id

-- *** Cursor

data Cursor = Cursor FullCollection BatchSize (MVar DelayedBatch)
-- ^ Iterator over results of a query. Use 'next' to iterate or 'rest' to get all results. A cursor is closed when it is explicitly closed, all results have been read from it, garbage collected, or not used for over 10 minutes (unless 'NoCursorTimeout' option was specified in 'Query'). Reading from a closed cursor raises a 'CursorNotFoundFailure'. Note, a cursor is not closed when the pipe is closed, so you can open another pipe to the same server and continue using the cursor.

newCursor :: (MonadIO m, MonadBaseControl IO m) => Database -> Collection -> BatchSize -> DelayedBatch -> Action m Cursor
-- ^ Create new cursor. If you don't read all results then close it. Cursor will be closed automatically when all results are read from it or when eventually garbage collected.
newCursor db col batchSize dBatch = do
	var <- newMVar dBatch
	let cursor = Cursor (db <.> col) batchSize var
	mkWeakMVar var (closeCursor cursor)
	return cursor
#if !MIN_VERSION_base(4,6,0)
  where mkWeakMVar = addMVarFinalizer
#endif

nextBatch :: (MonadIO m, MonadBaseControl IO m) => Cursor -> Action m [Document]
-- ^ Return next batch of documents in query result, which will be empty if finished.
nextBatch (Cursor fcol batchSize var) = modifyMVar var $ \dBatch -> do
	-- Pre-fetch next batch promise from server and return current batch.
	Batch limit cid docs <- fulfill' fcol batchSize dBatch
	dBatch' <- if cid /= 0 then nextBatch' fcol batchSize limit cid else return $ return (Batch 0 0 [])
	return (dBatch', docs)

fulfill' :: (MonadIO m) => FullCollection -> BatchSize -> DelayedBatch -> Action m Batch
-- Discard pre-fetched batch if empty with nonzero cid.
fulfill' fcol batchSize dBatch = do
	b@(Batch limit cid docs) <- fulfill dBatch
	if cid /= 0 && null docs
		then nextBatch' fcol batchSize limit cid >>= fulfill
		else return b

nextBatch' :: (MonadIO m) => FullCollection -> BatchSize -> Limit -> CursorId -> Action m DelayedBatch
nextBatch' fcol batchSize limit cid = request [] (GetMore fcol batchSize' cid, remLimit)
	where (batchSize', remLimit) = batchSizeRemainingLimit batchSize limit

next :: (MonadIO m, MonadBaseControl IO m) => Cursor -> Action m (Maybe Document)
-- ^ Return next document in query result, or Nothing if finished.
next (Cursor fcol batchSize var) = modifyMVar var nextState where
	-- Pre-fetch next batch promise from server when last one in current batch is returned.
	-- nextState:: DelayedBatch -> Action m (DelayedBatch, Maybe Document)
	nextState dBatch = do
		Batch limit cid docs <- fulfill' fcol batchSize dBatch
		case docs of
			doc : docs' -> do
				dBatch' <- if null docs' && cid /= 0
					then nextBatch' fcol batchSize limit cid
					else return $ return (Batch limit cid docs')
				return (dBatch', Just doc)
			[] -> if cid == 0
				then return (return $ Batch 0 0 [], Nothing)  -- finished
				else fmap (,Nothing) $ nextBatch' fcol batchSize limit cid

nextN :: (MonadIO m, MonadBaseControl IO m, Functor m) => Int -> Cursor -> Action m [Document]
-- ^ Return next N documents or less if end is reached
nextN n c = catMaybes <$> replicateM n (next c)

rest :: (MonadIO m, MonadBaseControl IO m, Functor m) => Cursor -> Action m [Document]
-- ^ Return remaining documents in query result
rest c = loop (next c)

closeCursor :: (MonadIO m, MonadBaseControl IO m) => Cursor -> Action m ()
closeCursor (Cursor _ _ var) = modifyMVar var $ \dBatch -> do
	Batch _ cid _ <- fulfill dBatch
	unless (cid == 0) $ send [KillCursors [cid]]
	return $ (return $ Batch 0 0 [], ())

isCursorClosed :: (MonadIO m, MonadBase IO m) => Cursor -> Action m Bool
isCursorClosed (Cursor _ _ var) = do
		Batch _ cid docs <- fulfill =<< readMVar var
		return (cid == 0 && null docs)

-- ** Aggregate

type Pipeline = [Document]
-- ^ The Aggregate Pipeline

aggregate :: MonadIO' m => Collection -> Pipeline -> Action m [Document]
-- ^ Runs an aggregate and unpacks the result. See <http://docs.mongodb.org/manual/core/aggregation/> for details.
aggregate aColl agg = do
	response <- runCommand ["aggregate" =: aColl, "pipeline" =: agg]
	case true1 "ok" response of
		True  -> lookup "result" response
		False -> throwError $ AggregateFailure $ at "errmsg" response

-- ** Group

-- | Groups documents in collection by key then reduces (aggregates) each group
data Group = Group {
	gColl :: Collection,
	gKey :: GroupKey,  -- ^ Fields to group by
	gReduce :: Javascript,  -- ^ @(doc, agg) -> ()@. The reduce function reduces (aggregates) the objects iterated. Typical operations of a reduce function include summing and counting. It takes two arguments, the current document being iterated over and the aggregation value, and updates the aggregate value.
	gInitial :: Document,  -- ^ @agg@. Initial aggregation value supplied to reduce
	gCond :: Selector,  -- ^ Condition that must be true for a row to be considered. [] means always true.
	gFinalize :: Maybe Javascript  -- ^ @agg -> () | result@. An optional function to be run on each item in the result set just before the item is returned. Can either modify the item (e.g., add an average field given a count and a total) or return a replacement object (returning a new object with just _id and average fields).
	} deriving (Show, Eq)

data GroupKey = Key [Label] | KeyF Javascript  deriving (Show, Eq)
-- ^ Fields to group by, or function (@doc -> key@) returning a "key object" to be used as the grouping key. Use KeyF instead of Key to specify a key that is not an existing member of the object (or, to access embedded members).

groupDocument :: Group -> Document
-- ^ Translate Group data into expected document form
groupDocument Group{..} =
	("finalize" =? gFinalize) ++ [
	"ns" =: gColl,
	case gKey of Key k -> "key" =: map (=: True) k; KeyF f -> "$keyf" =: f,
	"$reduce" =: gReduce,
	"initial" =: gInitial,
	"cond" =: gCond ]

group :: (MonadIO' m) => Group -> Action m [Document]
-- ^ Execute group query and return resulting aggregate value for each distinct key
group g = at "retval" <$> runCommand ["group" =: groupDocument g]

-- ** MapReduce

-- | Maps every document in collection to a list of (key, value) pairs, then for each unique key reduces all its associated values to a single result. There are additional parameters that may be set to tweak this basic operation.
-- This implements the latest version of map-reduce that requires MongoDB 1.7.4 or greater. To map-reduce against an older server use runCommand directly as described in http://www.mongodb.org/display/DOCS/MapReduce.
data MapReduce = MapReduce {
	rColl :: Collection,
	rMap :: MapFun,
	rReduce :: ReduceFun,
	rSelect :: Selector,  -- ^ Operate on only those documents selected. Default is [] meaning all documents.
	rSort :: Order,  -- ^ Default is [] meaning no sort
	rLimit :: Limit,  -- ^ Default is 0 meaning no limit
	rOut :: MROut,  -- ^ Output to a collection with a certain merge policy. Default is no collection ('Inline'). Note, you don't want this default if your result set is large.
	rFinalize :: Maybe FinalizeFun,  -- ^ Function to apply to all the results when finished. Default is Nothing.
	rScope :: Document,  -- ^ Variables (environment) that can be accessed from map/reduce/finalize. Default is [].
	rVerbose :: Bool  -- ^ Provide statistics on job execution time. Default is False.
	} deriving (Show, Eq)

type MapFun = Javascript
-- ^ @() -> void@. The map function references the variable @this@ to inspect the current object under consideration. The function must call @emit(key,value)@ at least once, but may be invoked any number of times, as may be appropriate.

type ReduceFun = Javascript
-- ^ @(key, [value]) -> value@. The reduce function receives a key and an array of values and returns an aggregate result value. The MapReduce engine may invoke reduce functions iteratively; thus, these functions must be idempotent.  That is, the following must hold for your reduce function: @reduce(k, [reduce(k,vs)]) == reduce(k,vs)@. If you need to perform an operation only once, use a finalize function. The output of emit (the 2nd param) and reduce should be the same format to make iterative reduce possible.

type FinalizeFun = Javascript
-- ^ @(key, value) -> final_value@. A finalize function may be run after reduction.  Such a function is optional and is not necessary for many map/reduce cases.  The finalize function takes a key and a value, and returns a finalized value.

data MROut =
	  Inline -- ^ Return results directly instead of writing them to an output collection. Results must fit within 16MB limit of a single document
	| Output MRMerge Collection (Maybe Database) -- ^ Write results to given collection, in other database if specified. Follow merge policy when entry already exists
	deriving (Show, Eq)

data MRMerge =
	  Replace  -- ^ Clear all old data and replace it with new data
	| Merge  -- ^ Leave old data but overwrite entries with the same key with new data
	| Reduce  -- ^ Leave old data but combine entries with the same key via MR's reduce function
	deriving (Show, Eq)

type MRResult = Document
-- ^ Result of running a MapReduce has some stats besides the output. See http://www.mongodb.org/display/DOCS/MapReduce#MapReduce-Resultobject

mrDocument :: MapReduce -> Document
-- ^ Translate MapReduce data into expected document form
mrDocument MapReduce{..} =
	("mapreduce" =: rColl) :
	("out" =: mrOutDoc rOut) :
	("finalize" =? rFinalize) ++ [
	"map" =: rMap,
	"reduce" =: rReduce,
	"query" =: rSelect,
	"sort" =: rSort,
	"limit" =: (fromIntegral rLimit :: Int),
	"scope" =: rScope,
	"verbose" =: rVerbose ]

mrOutDoc :: MROut -> Document
-- ^ Translate MROut into expected document form
mrOutDoc Inline = ["inline" =: (1 :: Int)]
mrOutDoc (Output mrMerge coll mDB) = (mergeName mrMerge =: coll) : mdb mDB where
	mergeName Replace = "replace"
	mergeName Merge = "merge"
	mergeName Reduce = "reduce"
	mdb Nothing = []
	mdb (Just db) = ["db" =: db]

mapReduce :: Collection -> MapFun -> ReduceFun -> MapReduce
-- ^ MapReduce on collection with given map and reduce functions. Remaining attributes are set to their defaults, which are stated in their comments.
mapReduce col map' red = MapReduce col map' red [] [] 0 Inline Nothing [] False

runMR :: (MonadIO m, MonadBaseControl IO m, Applicative m) => MapReduce -> Action m Cursor
-- ^ Run MapReduce and return cursor of results. Error if map/reduce fails (because of bad Javascript)
runMR mr = do
	res <- runMR' mr
	case look "result" res of
		Just (String coll) -> find $ query [] coll
		Just (Doc doc) -> useDb (at "db" doc) $ find $ query [] (at "collection" doc)
		Just x -> error $ "unexpected map-reduce result field: " ++ show x
		Nothing -> newCursor "" "" 0 $ return $ Batch 0 0 (at "results" res)

runMR' :: (MonadIO' m) => MapReduce -> Action m MRResult
-- ^ Run MapReduce and return a MR result document containing stats and the results if Inlined. Error if the map/reduce failed (because of bad Javascript).
runMR' mr = do
	doc <- runCommand (mrDocument mr)
	return $ if true1 "ok" doc then doc else error $ "mapReduce error:\n" ++ show doc ++ "\nin:\n" ++ show mr

-- * Command

type Command = Document
-- ^ A command is a special query or action against the database. See <http://www.mongodb.org/display/DOCS/Commands> for details.

runCommand :: (MonadIO' m) => Command -> Action m Document
-- ^ Run command against the database and return its result
runCommand c = maybe err id <$> findOne (query c "$cmd") where
	err = error $ "Nothing returned for command: " ++ show c

runCommand1 :: (MonadIO' m) => Text -> Action m Document
-- ^ @runCommand1 foo = runCommand [foo =: 1]@
runCommand1 c = runCommand [c =: (1 :: Int)]

eval :: (MonadIO' m, Val v) => Javascript -> Action m v
-- ^ Run code on server
eval code = at "retval" <$> runCommand ["$eval" =: code]


{- Authors: Tony Hannan <tony@10gen.com>
   Copyright 2011 10gen Inc.
   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. -}
