-- | Query and update documents

{-# LANGUAGE OverloadedStrings, RecordWildCards, NamedFieldPuns, TupleSections, FlexibleContexts, FlexibleInstances, UndecidableInstances, MultiParamTypeClasses, GeneralizedNewtypeDeriving, StandaloneDeriving, TypeSynonymInstances, RankNTypes #-}

module Database.MongoDB.Query (
	-- * Access
	access, Access, Action, runAction, Failure(..),
	-- * Database
	Database(..), allDatabases, DbAccess, use, thisDatabase,
	-- ** Authentication
	P.Username, P.Password, auth,
	-- * Collection
	Collection, allCollections,
	-- ** Selection
	Selection(..), Selector, whereJS,
	Select(select),
	-- * Write
	-- ** WriteMode
	WriteMode(..), safe, GetLastError, writeMode,
	-- ** Insert
	insert, insert_, insertMany, insertMany_,
	-- ** Update
	save, replace, repsert, Modifier, modify,
	-- ** Delete
	delete, deleteOne,
	-- * Read
	readMode,
	-- ** Query
	Query(..), QueryOption(..), Projector, Limit, Order, BatchSize,
	explain, find, findOne, count, distinct,
	-- *** Cursor
	Cursor, next, nextN, rest, closeCursor, isCursorClosed,
	-- ** Group
	Group(..), GroupKey(..), group,
	-- ** MapReduce
	MapReduce(..), MapFun, ReduceFun, FinalizeFun, MROut(..), MRMerge(..), MRResult, mapReduce, runMR, runMR',
	-- * Command
	Command, runCommand, runCommand1,
	eval,
) where

import Prelude as X hiding (lookup)
import Control.Applicative ((<$>), Applicative(..))
import Control.Arrow (first)
import Control.Monad.Context
import Control.Monad.Reader
import Control.Monad.Error
import Control.Monad.Throw
import Control.Monad.MVar
import qualified Database.MongoDB.Internal.Protocol as P
import Database.MongoDB.Internal.Protocol hiding (Query, QueryOption(..), send, call)
import Database.MongoDB.Connection (MasterOrSlaveOk(..), Service(..))
import Data.Bson
import Data.Word
import Data.Int
import Data.Maybe (listToMaybe, catMaybes)
import Data.UString as U (dropWhile, any, tail)
import Control.Monad.Util (MonadIO', loop)
import Database.MongoDB.Internal.Util ((<.>), true1)

mapErrorIO :: (Throw e m, MonadIO m) => (e' -> e) -> ErrorT e' IO a -> m a
mapErrorIO f = throwLeft' f . liftIO . runErrorT

-- * Mongo Monad

access :: (Service s, MonadIO m) => WriteMode -> MasterOrSlaveOk -> ConnPool s -> Action m a -> m (Either Failure a)
-- ^ Run action under given write and read mode against the server or replicaSet behind given connection pool. Return Left Failure if there is a connection failure or read/write error.
access w mos pool act = do
	ePipe <- liftIO . runErrorT $ getPipe mos pool
	either (return . Left . ConnectionFailure) (runAction act w mos) ePipe

-- | A monad with access to a 'Pipe', 'MasterOrSlaveOk', and 'WriteMode', and throws 'Failure' on read, write, or pipe failure
class (Context Pipe m, Context MasterOrSlaveOk m, Context WriteMode m, Throw Failure m, MonadIO' m) => Access m
instance (Context Pipe m, Context MasterOrSlaveOk m, Context WriteMode m, Throw Failure m, MonadIO' m) => Access m

wrapIO :: (Access m) => (WriteMode -> MasterOrSlaveOk -> Pipe -> IO (Either Failure a)) -> m a
-- ^ Lift IO with Access context and failure into Access monad
wrapIO act = do
	writeMod <- context
	mos <- context
	pipe <- context
	e <- liftIO (act writeMod mos pipe)
	either throw return e

modifyMVar' :: (Access m) => MVar a -> (a -> Action IO (a, b)) -> m b
modifyMVar' var act = wrapIO $ \w m p -> modifyMVar var $ \a -> do
	e <- runAction (act a) w m p
	return $ either ((a,) . Left) (Right <$>) e
	
addMVarFinalizer' :: (Access m) => MVar a -> Action IO () -> m ()
addMVarFinalizer' var act = wrapIO $ \w m p -> do
	addMVarFinalizer var $ runAction act w m p >> return ()  -- ignore any failure
	return (Right ())

newtype Action m a = Action (ErrorT Failure (ReaderT WriteMode (ReaderT MasterOrSlaveOk (ReaderT Pipe m))) a)
	deriving (Context Pipe, Context MasterOrSlaveOk, Context WriteMode, Throw Failure, MonadIO, Monad, Applicative, Functor)
-- ^ Monad with access to a 'Pipe', 'MasterOrSlaveOk', and 'WriteMode', and throws a 'Failure' on read, write or pipe failure

instance MonadTrans Action where
	lift = Action . lift . lift . lift . lift

runAction :: Action m a -> WriteMode -> MasterOrSlaveOk -> Pipe -> m (Either Failure a)
-- ^ Run action with given write mode and read mode (master or slave-ok) against given pipe (TCP connection). Return Left Failure if read/write error or connection failure.
-- 'access' calls runAction. Use this directly if you want to use the same connection and not take from the pool again. However, the connection may still be used by other threads at the same time. For instance, the pool will still hand this connection out.
runAction (Action action) w mos = runReaderT (runReaderT (runReaderT (runErrorT action) w) mos)

-- | A connection failure, or a read or write exception like cursor expired or inserting a duplicate key.
-- Note, unexpected data from the server is not a Failure, rather it is a programming error (you should call 'error' in this case) because the client and server are incompatible and requires a programming change.
data Failure =
	 ConnectionFailure IOError  -- ^ TCP connection ('Pipe') failed. Make work if you try again on the same Mongo 'Connection' which will create a new Pipe.
	| CursorNotFoundFailure CursorId  -- ^ Cursor expired because it wasn't accessed for over 10 minutes, or this cursor came from a different server that the one you are currently connected to (perhaps a fail over happen between servers in a replica set)
	| QueryFailure String  -- ^ Query failed for some reason as described in the string
	| WriteFailure ErrorCode String  -- ^ Error observed by getLastError after a write, error description is in string
	deriving (Show, Eq)

instance Error Failure where strMsg = error
-- ^ 'fail' is treated the same as 'error'. In other words, don't use it.

-- * Database

newtype Database = Database {databaseName :: UString}  deriving (Eq, Ord)
-- ^ Database name

instance Show Database where show (Database x) = unpack x

-- | 'Access' monad with a particular 'Database' in context
class (Context Database m, Access m) => DbAccess m
instance (Context Database m, Access m) => DbAccess m

allDatabases :: (Access m) => m [Database]
-- ^ List all databases residing on server
allDatabases = map (Database . at "name") . at "databases" <$> use (Database "admin") (runCommand1 "listDatabases")

use :: Database -> ReaderT Database m a -> m a
-- ^ Run action against given database
use = flip runReaderT

thisDatabase :: (DbAccess m) => m Database
-- ^ Current database in use
thisDatabase = context

-- * Authentication

auth :: (DbAccess m) => Username -> Password -> m Bool
-- ^ Authenticate with the database (if server is running in secure mode). Return whether authentication was successful or not. Reauthentication is required for every new pipe.
auth usr pss = do
	n <- at "nonce" <$> runCommand ["getnonce" =: (1 :: Int)]
	true1 "ok" <$> runCommand ["authenticate" =: (1 :: Int), "user" =: usr, "nonce" =: n, "key" =: pwKey n usr pss]

-- * Collection

type Collection = UString
-- ^ Collection name (not prefixed with database)

allCollections :: (DbAccess m) => m [Collection]
-- ^ List all collections in this database
allCollections = do
	db <- thisDatabase
	docs <- rest =<< find (query [] "system.namespaces") {sort = ["name" =: (1 :: Int)]}
	return . filter (not . isSpecial db) . map dropDbPrefix $ map (at "name") docs
 where
 	dropDbPrefix = U.tail . U.dropWhile (/= '.')
 	isSpecial (Database db) col = U.any (== '$') col && db <.> col /= "local.oplog.$main"

-- * Selection

data Selection = Select {selector :: Selector, coll :: Collection}  deriving (Show, Eq)
-- ^ Selects documents in collection that match selector

type Selector = Document
-- ^ Filter for a query, analogous to the where clause in SQL. @[]@ matches all documents in collection. @[x =: a, y =: b]@ is analogous to @where x = a and y = b@ in SQL. See <http://www.mongodb.org/display/DOCS/Querying> for full selector syntax.

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

-- ** WriteMode

-- | Default write-mode is 'Unsafe'
data WriteMode =
	  Unsafe  -- ^ Submit writes without receiving acknowledgments. Fast. Assumes writes succeed even though they may not.
	| Safe GetLastError  -- ^ Receive an acknowledgment after every write, and raise exception if one says the write failed. This is acomplished by sending the getLastError command, with given 'GetLastError' parameters, after every write.
	deriving (Show, Eq)

type GetLastError = Document
-- ^ Parameters for getLastError command. For example ["w" =: 2] tells the server to wait for the write to reach at least two servers in replica set before acknowledging. See "http://www.mongodb.org/display/DOCS/Last+Error+Commands" for more options.

safe :: WriteMode
-- ^ Safe []
safe = Safe []

writeMode :: (Access m) => WriteMode -> m a -> m a
-- ^ Run action with given 'WriteMode'
writeMode = push . const

write :: (DbAccess m) => Notice -> m ()
-- ^ Send write to server, and if write-mode is 'Safe' then include getLastError request and raise 'WriteFailure' if it reports an error.
write notice = do
	mode <- context
	case mode of
		Unsafe -> send [notice]
		Safe params -> do
			me <- getLastError [notice] params
			maybe (return ()) (throw . uncurry WriteFailure) me

type ErrorCode = Int
-- ^ Error code from getLastError

getLastError :: (DbAccess m) => [Notice] -> GetLastError -> m (Maybe (ErrorCode, String))
-- ^ Send notices (writes) then fetch what the last error was, Nothing means no error
getLastError writes params = do
	r <- runCommand' writes $ ("getlasterror" =: (1 :: Int)) : params
	return $ (at "code" r,) <$> lookup "err" r

{-resetLastError :: (DbConn m) => m ()
-- ^ Clear last error
resetLastError = runCommand1 "reseterror" >> return ()-}

-- ** Insert

insert :: (DbAccess m) => Collection -> Document -> m Value
-- ^ Insert document into collection and return its \"_id\" value, which is created automatically if not supplied
insert col doc = head <$> insertMany col [doc]

insert_ :: (DbAccess m) => Collection -> Document -> m ()
-- ^ Same as 'insert' except don't return _id
insert_ col doc = insert col doc >> return ()

insertMany :: (DbAccess m) => Collection -> [Document] -> m [Value]
-- ^ Insert documents into collection and return their \"_id\" values, which are created automatically if not supplied
insertMany col docs = do
	Database db <- thisDatabase
	docs' <- liftIO $ mapM assignId docs
	write (Insert (db <.> col) docs')
	mapM (look "_id") docs'

insertMany_ :: (DbAccess m) => Collection -> [Document] -> m ()
-- ^ Same as 'insertMany' except don't return _ids
insertMany_ col docs = insertMany col docs >> return ()

assignId :: Document -> IO Document
-- ^ Assign a unique value to _id field if missing
assignId doc = if X.any (("_id" ==) . label) doc
	then return doc
	else (\oid -> ("_id" =: oid) : doc) <$> genObjectId

-- ** Update 

save :: (DbAccess m) => Collection -> Document -> m ()
-- ^ Save document to collection, meaning insert it if its new (has no \"_id\" field) or update it if its not new (has \"_id\" field)
save col doc = case look "_id" doc of
	Nothing -> insert_ col doc
	Just i -> repsert (Select ["_id" := i] col) doc

replace :: (DbAccess m) => Selection -> Document -> m ()
-- ^ Replace first document in selection with given document
replace = update []

repsert :: (DbAccess m) => Selection -> Document -> m ()
-- ^ Replace first document in selection with given document, or insert document if selection is empty
repsert = update [Upsert]

type Modifier = Document
-- ^ Update operations on fields in a document. See <http://www.mongodb.org/display/DOCS/Updating#Updating-ModifierOperations>

modify :: (DbAccess m) => Selection -> Modifier -> m ()
-- ^ Update all documents in selection using given modifier
modify = update [MultiUpdate]

update :: (DbAccess m) => [UpdateOption] -> Selection -> Document -> m ()
-- ^ Update first document in selection using updater document, unless 'MultiUpdate' option is supplied then update all documents in selection. If 'Upsert' option is supplied then treat updater as document and insert it if selection is empty.
update opts (Select sel col) up = do
	Database db <- thisDatabase
	write (Update (db <.> col) opts sel up)

-- ** Delete

delete :: (DbAccess m) => Selection -> m ()
-- ^ Delete all documents in selection
delete = delete' []

deleteOne :: (DbAccess m) => Selection -> m ()
-- ^ Delete first document in selection
deleteOne = delete' [SingleRemove]

delete' :: (DbAccess m) => [DeleteOption] -> Selection -> m ()
-- ^ Delete all documents in selection unless 'SingleRemove' option is given then only delete first document in selection
delete' opts (Select sel col) = do
	Database db <- thisDatabase
	write (Delete (db <.> col) opts sel)

-- * Read

-- ** MasterOrSlaveOk

readMode :: (Access m) => MasterOrSlaveOk -> m a -> m a
-- ^ Execute action using given read mode. Master = consistent reads, SlaveOk = eventually consistent reads.
readMode = push . const

msOption :: MasterOrSlaveOk -> [P.QueryOption]
msOption Master = []
msOption SlaveOk = [P.SlaveOK]

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

data QueryOption =
	  TailableCursor  -- ^ Tailable means cursor is not closed when the last data is retrieved. Rather, the cursor marks the final object's position. You can resume using the cursor later, from where it was located, if more data were received. Like any "latent cursor", the cursor may become invalid at some point – for example if the final object it references were deleted. Thus, you should be prepared to requery on CursorNotFound exception.
	| NoCursorTimeout  -- The server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use. Set this option to prevent that.
	| AwaitData  -- ^ Use with TailableCursor. If we are at the end of the data, block for a while rather than returning no data. After a timeout period, we do return as normal.
	deriving (Show, Eq)

pOption :: QueryOption -> P.QueryOption
-- ^ Convert to protocol query option
pOption TailableCursor = P.TailableCursor
pOption NoCursorTimeout = P.NoCursorTimeout
pOption AwaitData = P.AwaitData

type Projector = Document
-- ^ Fields to return, analogous to the select clause in SQL. @[]@ means return whole document (analogous to * in SQL). @[x =: 1, y =: 1]@ means return only @x@ and @y@ fields of each document. @[x =: 0]@ means return all fields except @x@.

type Limit = Word32
-- ^ Maximum number of documents to return, i.e. cursor will close after iterating over this number of documents. 0 means no limit.

type Order = Document
-- ^ Fields to sort by. Each one is associated with 1 or -1. Eg. @[x =: 1, y =: -1]@ means sort by @x@ ascending then @y@ descending

type BatchSize = Word32
-- ^ The number of document to return in each batch response from the server. 0 means use Mongo default.

query :: Selector -> Collection -> Query
-- ^ Selects documents in collection that match selector. It uses no query options, projects all fields, does not skip any documents, does not limit result size, uses default batch size, does not sort, does not hint, and does not snapshot.
query sel col = Query [] (Select sel col) [] 0 0 [] False 0 []

batchSizeRemainingLimit :: BatchSize -> Limit -> (Int32, Limit)
-- ^ Given batchSize and limit return P.qBatchSize and remaining limit
batchSizeRemainingLimit batchSize limit = if limit == 0
	then (fromIntegral batchSize', 0)  -- no limit
	else if 0 < batchSize' && batchSize' < limit
		then (fromIntegral batchSize', limit - batchSize')
		else (- fromIntegral limit, 1)
 where batchSize' = if batchSize == 1 then 2 else batchSize
 	-- batchSize 1 is broken because server converts 1 to -1 meaning limit 1

queryRequest :: Bool -> MasterOrSlaveOk -> Query -> Database -> (Request, Limit)
-- ^ Translate Query to Protocol.Query. If first arg is true then add special $explain attribute.
queryRequest isExplain mos Query{..} (Database db) = (P.Query{..}, remainingLimit) where
	qOptions = msOption mos ++ map pOption options
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

runQuery :: (DbAccess m) => Bool -> [Notice] -> Query -> m DelayedCursorState
-- ^ Send query request and return cursor state
runQuery isExplain ns q = do
	db <- thisDatabase
	slaveOK <- context
	request ns (queryRequest isExplain slaveOK q db)

find :: (DbAccess m) => Query -> m Cursor
-- ^ Fetch documents satisfying query
find q@Query{selection, batchSize} = do
	db <- thisDatabase
	dcs <- runQuery False [] q
	newCursor db (coll selection) batchSize dcs

findOne' :: (DbAccess m) => [Notice] -> Query -> m (Maybe Document)
-- ^ Send notices and fetch first document satisfying query or Nothing if none satisfy it
findOne' ns q = do
	CS _ _ docs <- mapErrorIO id =<< runQuery False ns q {limit = 1}
	return (listToMaybe docs)

findOne :: (DbAccess m) => Query -> m (Maybe Document)
-- ^ Fetch first document satisfying query or Nothing if none satisfy it
findOne = findOne' []

explain :: (DbAccess m) => Query -> m Document
-- ^ Return performance stats of query execution
explain q = do  -- same as findOne but with explain set to true
	CS _ _ docs <- mapErrorIO id =<< runQuery True [] q {limit = 1}
	return $ if null docs then error ("no explain: " ++ show q) else head docs

count :: (DbAccess m) => Query -> m Int
-- ^ Fetch number of documents satisfying query (including effect of skip and/or limit if present)
count Query{selection = Select sel col, skip, limit} = at "n" <$> runCommand
	(["count" =: col, "query" =: sel, "skip" =: (fromIntegral skip :: Int32)]
		++ ("limit" =? if limit == 0 then Nothing else Just (fromIntegral limit :: Int32)))

distinct :: (DbAccess m) => Label -> Selection -> m [Value]
-- ^ Fetch distinct values of field in selected documents
distinct k (Select sel col) = at "values" <$> runCommand ["distinct" =: col, "key" =: k, "query" =: sel]

-- *** Cursor

data Cursor = Cursor FullCollection BatchSize (MVar DelayedCursorState)
-- ^ Iterator over results of a query. Use 'next' to iterate or 'rest' to get all results. A cursor is closed when it is explicitly closed, all results have been read from it, garbage collected, or not used for over 10 minutes (unless 'NoCursorTimeout' option was specified in 'Query'). Reading from a closed cursor raises a 'CursorNotFoundFailure'. Note, a cursor is not closed when the pipe is closed, so you can open another pipe to the same server and continue using the cursor.

getCursorState :: (Access m) => Cursor -> m CursorState
-- ^ Extract current cursor status
getCursorState (Cursor _ _ var) = mapErrorIO id =<< readMVar var

type DelayedCursorState = ErrorT Failure IO CursorState
-- ^ A promised cursor state which may fail

request :: (Access m) => [Notice] -> (Request, Limit) -> m DelayedCursorState
-- ^ Send notices and request and return promised cursor state
request ns (req, remainingLimit) = do
	promise <- call ns req
	return $ fromReply remainingLimit =<< promise

data CursorState = CS Limit CursorId [Document]
-- ^ CursorId = 0 means cursor is finished. Documents is remaining documents to serve in current batch. Limit is remaining limit for next fetch.

fromReply :: (Throw Failure m) => Limit -> Reply -> m CursorState
-- ^ Convert Reply to CursorState or Failure
fromReply limit Reply{..} = do
	mapM_ checkResponseFlag rResponseFlags
	return (CS limit rCursorId rDocuments)
 where
	-- If response flag indicates failure then throw it, otherwise do nothing
	checkResponseFlag flag = case flag of
		AwaitCapable -> return ()
		CursorNotFound -> throw (CursorNotFoundFailure rCursorId)
		QueryError -> throw (QueryFailure $ at "$err" $ head rDocuments)

newCursor :: (Access m) => Database -> Collection -> BatchSize -> DelayedCursorState -> m Cursor
-- ^ Create new cursor. If you don't read all results then close it. Cursor will be closed automatically when all results are read from it or when eventually garbage collected.
newCursor (Database db) col batch cs = do
	var <- newMVar cs
	let cursor = Cursor (db <.> col) batch var
	addMVarFinalizer' var (closeCursor cursor)
	return cursor

next :: (Access m) => Cursor -> m (Maybe Document)
-- ^ Return next document in query result, or Nothing if finished.
next (Cursor fcol batch var) = modifyMVar' var nextState where
	-- Pre-fetch next batch promise from server when last one in current batch is returned.
	nextState:: DelayedCursorState -> Action IO (DelayedCursorState, Maybe Document)
	nextState dcs = do
		CS limit cid docs <- mapErrorIO id dcs
		case docs of
			doc : docs' -> do
				dcs' <- if null docs' && cid /= 0
					then nextBatch limit cid
					else return $ return (CS limit cid docs')
				return (dcs', Just doc)
			[] -> if cid == 0
				then return (return $ CS 0 0 [], Nothing)  -- finished
				else error $ "server returned empty batch but says more results on server"
	nextBatch limit cid = request [] (GetMore fcol batchSize cid, remLimit)
		where (batchSize, remLimit) = batchSizeRemainingLimit batch limit

nextN :: (Access m) => Int -> Cursor -> m [Document]
-- ^ Return next N documents or less if end is reached
nextN n c = catMaybes <$> replicateM n (next c)

rest :: (Access m) => Cursor -> m [Document]
-- ^ Return remaining documents in query result
rest c = loop (next c)

closeCursor :: (Access m) => Cursor -> m ()
closeCursor (Cursor _ _ var) = modifyMVar' var kill' where
 	kill' dcs = first return <$> (kill =<< mapErrorIO id dcs)
	kill (CS _ cid _) = (CS 0 0 [],) <$> if cid == 0 then return () else send [KillCursors [cid]]

isCursorClosed :: (Access m) => Cursor -> m Bool
isCursorClosed cursor = do
		CS _ cid docs <- getCursorState cursor
		return (cid == 0 && null docs)

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

group :: (DbAccess m) => Group -> m [Document]
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
	rOut :: MROut,  -- ^ Output to a collection with a certain merge policy. Default is no collection (Inline). Note, you don't want this default if your result set is large.
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
	mdb (Just (Database db)) = ["db" =: db]

mapReduce :: Collection -> MapFun -> ReduceFun -> MapReduce
-- ^ MapReduce on collection with given map and reduce functions. Remaining attributes are set to their defaults, which are stated in their comments.
mapReduce col map' red = MapReduce col map' red [] [] 0 Inline Nothing [] False

runMR :: (DbAccess m) => MapReduce -> m Cursor
-- ^ Run MapReduce and return cursor of results. Error if map/reduce fails (because of bad Javascript)
runMR mr = do
	res <- runMR' mr
	case look "result" res of
		Just (String coll) -> find $ query [] coll
		Just (Doc doc) -> use (Database $ at "db" doc) $ find $ query [] (at "collection" doc)
		Just x -> error $ "unexpected map-reduce result field: " ++ show x
		Nothing -> newCursor (Database "") "" 0 $ return $ CS 0 0 (at "results" res)

runMR' :: (DbAccess m) => MapReduce -> m MRResult
-- ^ Run MapReduce and return a MR result document containing stats and the results if Inlined. Error if the map/reduce failed (because of bad Javascript).
runMR' mr = do
	doc <- runCommand (mrDocument mr)
	return $ if true1 "ok" doc then doc else error $ "mapReduce error:\n" ++ show doc ++ "\nin:\n" ++ show mr

-- * Command

type Command = Document
-- ^ A command is a special query or action against the database. See <http://www.mongodb.org/display/DOCS/Commands> for details.

runCommand' :: (DbAccess m) => [Notice] -> Command -> m Document
-- ^ Send notices then run command and return its result
runCommand' ns c = maybe err id <$> findOne' ns (query c "$cmd") where
	err = error $ "Nothing returned for command: " ++ show c

runCommand :: (DbAccess m) => Command -> m Document
-- ^ Run command against the database and return its result
runCommand = runCommand' []

runCommand1 :: (DbAccess m) => UString -> m Document
-- ^ @runCommand1 foo = runCommand [foo =: 1]@
runCommand1 c = runCommand [c =: (1 :: Int)]

eval :: (DbAccess m) => Javascript -> m Document
-- ^ Run code on server
eval code = at "retval" <$> runCommand ["$eval" =: code]

-- * Primitives

send :: (Context Pipe m, Throw Failure m, MonadIO m) => [Notice] -> m ()
-- ^ Send notices as a contiguous batch to server with no reply. Throw 'ConnectionFailure' if pipe fails.
send ns = do
	pipe <- context
	mapErrorIO ConnectionFailure (P.send pipe ns)

call :: (Context Pipe m, Throw Failure m, MonadIO m, Throw Failure n, MonadIO n) =>
	[Notice] -> Request -> m (n Reply)
-- ^ Send notices and request as a contiguous batch to server and return reply promise, which will block when invoked until reply arrives. This call will throw 'ConnectionFailure' if pipe fails on send, and promise will throw 'ConnectionFailure' if pipe fails on receive.
call ns r = do
	pipe <- context
	promise <- mapErrorIO ConnectionFailure (P.call pipe ns r)
	return (mapErrorIO ConnectionFailure promise)


{- Authors: Tony Hannan <tony@10gen.com>
   Copyright 2010 10gen Inc.
   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. -}
