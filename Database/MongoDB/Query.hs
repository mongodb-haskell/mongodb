-- | Query and update documents residing on a MongoDB server(s)

{-# LANGUAGE OverloadedStrings, RecordWildCards, NamedFieldPuns, TupleSections #-}

module Database.MongoDB.Query (
	-- * Database
	Database, allDatabases, Db, useDb, thisDatabase, runDbOp,
	-- ** Authentication
	P.Username, P.Password, auth,
	-- * Collection
	Collection, allCollections,
	-- ** Selection
	Selection(..), select, Selector, whereJS,
	-- * Write
	-- ** Insert
	insert, insert_, insertMany, insertMany_,
	-- ** Update
	save, replace, repsert, Modifier, modify,
	-- ** Delete
	delete, deleteOne,
	-- * Read
	-- ** Query
	Query(..), P.QueryOption(..), Projector, Limit, Order, BatchSize, query,
	explain, find, findOne, count, distinct,
	-- *** Cursor
	Cursor, next, nextN, rest, closeCursor,
	-- ** Group
	Group(..), GroupKey(..), group,
	-- ** MapReduce
	MapReduce(..), MapFun, ReduceFun, FinalizeFun, mapReduce, runMR, runMR',
	-- * Command
	Command, runCommand, runCommand1,
	eval,
	ErrorCode, getLastError, resetLastError
) where

import Prelude as X hiding (lookup)
import Control.Applicative ((<$>))
import Database.MongoDB.Internal.Connection
import qualified Database.MongoDB.Internal.Protocol as P
import Database.MongoDB.Internal.Protocol hiding (insert, update, delete, query, Query(Query))
import Data.Bson
import Data.Word
import Data.Int
import Control.Monad.Reader
import Control.Concurrent.MVar
import Data.Maybe (listToMaybe, catMaybes)
import Data.UString as U (dropWhile, any, tail)
import Database.MongoDB.Util (loop, (<.>), true1)

-- * Database

type Database = UString
-- ^ Database name

allDatabases :: (Conn m) => m [Database]
-- ^ List all databases residing on server
allDatabases = map (at "name") . at "databases" <$> useDb "admin" (runCommand1 "listDatabases")

type Db m = ReaderT Database m

useDb :: Database -> Db m a -> m a
-- ^ Run Db action against given database
useDb = flip runReaderT

thisDatabase :: (Monad m) => Db m Database
-- ^ Current database in use
thisDatabase = ask

runDbOp :: (Conn m) => Db Op a -> Db m a
-- ^ Run db operation with exclusive access to the connection
runDbOp dbOp = ReaderT (runOp . flip useDb dbOp)

-- * Authentication

auth :: (Conn m) => Username -> Password -> Db m Bool
-- ^ Authenticate with the database (if server is running in secure mode). Return whether authentication was successful or not. Reauthentication is required for every new connection.
auth u p = do
	n <- at "nonce" <$> runCommand ["getnonce" =: (1 :: Int)]
	true1 "ok" <$> runCommand ["authenticate" =: (1 :: Int), "user" =: u, "nonce" =: n, "key" =: pwKey n u p]

-- * Collection

type Collection = UString
-- ^ Collection name (not prefixed with database)

allCollections :: (Conn m) => Db m [Collection]
-- ^ List all collections in this database
allCollections = do
	db <- thisDatabase
	docs <- rest =<< find (query [] "system.namespaces") {sort = ["name" =: (1 :: Int)]}
	return . filter (not . isSpecial db) . map dropDbPrefix $ map (at "name") docs
 where
 	dropDbPrefix = U.tail . U.dropWhile (/= '.')
 	isSpecial db col = U.any (== '$') col && db <.> col /= "local.oplog.$main"

-- * Selection

data Selection = Select {selector :: Selector, coll :: Collection}  deriving (Show, Eq)
-- ^ Selects documents in collection that match selector

select :: Selector -> Collection -> Selection
-- ^ Synonym for 'Select'
select = Select

type Selector = Document
-- ^ Filter for a query, analogous to the where clause in SQL. @[]@ matches all documents in collection. @[x =: a, y =: b]@ is analogous to @where x = a and y = b@ in SQL. See <http://www.mongodb.org/display/DOCS/Querying> for full selector syntax.

whereJS :: Selector -> Javascript -> Selector
-- ^ Add Javascript predicate to selector, in which case a document must match both selector and predicate
whereJS sel js = ("$where" =: js) : sel

-- * Write

-- ** Insert

insert :: (Conn m) => Collection -> Document -> Db m Value
-- ^ Insert document into collection and return its \"_id\" value, which is created automatically if not supplied
insert col doc = head <$> insertMany col [doc]

insert_ :: (Conn m) => Collection -> Document -> Db m ()
-- ^ Same as 'insert' except don't return _id
insert_ col doc = insert col doc >> return ()

insertMany :: (Conn m) => Collection -> [Document] -> Db m [Value]
-- ^ Insert documents into collection and return their \"_id\" values, which are created automatically if not supplied
insertMany col docs = ReaderT $ \db -> do
	docs' <- liftIO $ mapM assignId docs
	runOp $ P.insert (Insert (db <.> col) docs')
	mapM (look "_id") docs'

insertMany_ :: (Conn m) => Collection -> [Document] -> Db m ()
-- ^ Same as 'insertMany' except don't return _ids
insertMany_ col docs = insertMany col docs >> return ()

assignId :: Document -> IO Document
-- ^ Assign a unique value to _id field if missing
assignId doc = if X.any (("_id" ==) . label) doc
	then return doc
	else (\oid -> ("_id" =: oid) : doc) <$> genObjectId

-- ** Update 

save :: (Conn m) => Collection -> Document -> Db m ()
-- ^ Save document to collection, meaning insert it if its new (has no \"_id\" field) or update it if its not new (has \"_id\" field)
save col doc = case look "_id" doc of
	Nothing -> insert_ col doc
	Just i -> repsert (Select ["_id" := i] col) doc

replace :: (Conn m) => Selection -> Document -> Db m ()
-- ^ Replace first document in selection with given document
replace = update []

repsert :: (Conn m) => Selection -> Document -> Db m ()
-- ^ Replace first document in selection with given document, or insert document if selection is empty
repsert = update [Upsert]

type Modifier = Document
-- ^ Update operations on fields in a document. See <http://www.mongodb.org/display/DOCS/Updating#Updating-ModifierOperations>

modify :: (Conn m) => Selection -> Modifier -> Db m ()
-- ^ Update all documents in selection using given modifier
modify = update [MultiUpdate]

update :: (Conn m) => [UpdateOption] -> Selection -> Document -> Db m ()
-- ^ Update first document in selection using updater document, unless 'MultiUpdate' option is supplied then update all documents in selection. If 'Upsert' option is supplied then treat updater as document and insert it if selection is empty.
update opts (Select sel col) up = ReaderT $ \db -> runOp $ P.update (Update (db <.> col) opts sel up)

-- ** Delete

delete :: (Conn m) => Selection -> Db m ()
-- ^ Delete all documents in selection
delete (Select sel col) = ReaderT $ \db -> runOp $ P.delete (Delete (db <.> col) [] sel)

deleteOne :: (Conn m) => Selection -> Db m ()
-- ^ Delete first document in selection
deleteOne (Select sel col) = ReaderT $ \db -> runOp $ P.delete (Delete (db <.> col) [SingleRemove] sel)

-- * Read

-- ** Query

data Query = Query {
	options :: [QueryOption],
	selection :: Selection,
	project :: Projector,  -- ^ \[\] = all fields
	skip :: Word32,  -- ^ Number of initial matching documents to skip
	limit :: Limit, -- ^ Maximum number of documents to return, 0 = no limit
	sort :: Order,  -- ^ Sort results by this order, [] = no sort
	snapshot :: Bool,  -- ^ If true assures no duplicates are returned, or objects missed, which were present at both the start and end of the query's execution (even if the object were updated). If an object is new during the query, or deleted during the query, it may or may not be returned, even with snapshot mode. Note that short query responses (less than 1MB) are always effectively snapshotted.
	batchSize :: BatchSize,  -- ^ The number of document to return in each batch response from the server. 0 means use Mongo default.
	hint :: Order  -- ^ Force MongoDB to use this index, [] = no hint
	} deriving (Show, Eq)

type Projector = Document
-- ^ Fields to return, analogous to the select clause in SQL. @[]@ means return whole document (analogous to * in SQL). @[x =: 1, y =: 1]@ means return only @x@ and @y@ fields of each document. @[x =: 0]@ means return all fields except @x@.

type Limit = Word32
-- ^ Maximum number of documents to return, i.e. cursor will close after iterating over this number of documents. 0 means no limit.

type Order = Document
-- ^ Fields to sort by. Each one is associated with 1 or -1. Eg. @[x =: 1, y =: (-1)]@ means sort by @x@ ascending then @y@ descending

type BatchSize = Word32
-- ^ The number of document to return in each batch response from the server. 0 means use Mongo default.

query :: Selector -> Collection -> Query
-- ^ Selects documents in collection that match selector. It uses no query options, projects all fields, does not skip any documents, does not limit result size, uses default batch size, does not sort, does not hint, and does not snapshot.
query sel col = Query [] (Select sel col) [] 0 0 [] False 0 []

batchSizeRemainingLimit :: BatchSize -> Limit -> (Int32, Limit)
-- ^ Given batchSize and limit return P.qBatchSize and remaining limit
batchSizeRemainingLimit batchSize limit = if limit == 0
	then (fromIntegral batchSize, 0)  -- no limit
	else if 0 < batchSize && batchSize < limit
		then (fromIntegral batchSize, limit - batchSize)
		else (- fromIntegral limit, 1)

protoQuery :: Database -> Query -> (P.Query, Limit)
protoQuery = protoQuery' False

protoQuery' :: Bool -> Database -> Query -> (P.Query, Limit)
-- ^ Translate Query to Protocol.Query. If first arg is true then add special $explain attribute.
protoQuery' isExplain db Query{..} = (P.Query{..}, remainingLimit) where
	qOptions = options
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

find :: (Conn m) => Query -> Db m Cursor
-- ^ Fetch documents satisfying query
find q@Query{selection, batchSize} = ReaderT $ \db -> do
	let (q', remainingLimit) = protoQuery db q
	cs <- fromReply remainingLimit =<< runOp (P.query q')
	newCursor db (coll selection) batchSize cs

findOne :: (Conn m) => Query -> Db m (Maybe Document)
-- ^ Fetch first document satisfying query or Nothing if none satisfy it
findOne q = ReaderT $ \db -> do
	let (q', x) = protoQuery db q {limit = 1}
	CS _ _ docs <- fromReply x =<< runOp (P.query q')
	return (listToMaybe docs)

explain :: (Conn m) => Query -> Db m Document
-- ^ Return performance stats of query execution
explain q = ReaderT $ \db -> do  -- same as findOne but with explain set to true
	let (q', x) = protoQuery' True db q {limit = 1}
	CS _ _ docs <- fromReply x =<< runOp (P.query q')
	when (null docs) . fail $ "no explain: " ++ show q'
	return (head docs)

count :: (Conn m) => Query -> Db m Int
-- ^ Fetch number of documents satisfying query (including effect of skip and/or limit if present)
count Query{selection = Select sel col, skip, limit} = at "n" <$> runCommand
	(["count" =: col, "query" =: sel, "skip" =: (fromIntegral skip :: Int32)]
		++ ("limit" =? if limit == 0 then Nothing else Just (fromIntegral limit :: Int32)))

distinct :: (Conn m) => Label -> Selection -> Db m [Value]
-- ^ Fetch distinct values of field in selected documents
distinct k (Select sel col) = at "values" <$> runCommand ["distinct" =: col, "key" =: k, "query" =: sel]

-- *** Cursor

data Cursor = Cursor FullCollection BatchSize (MVar CursorState)
-- ^ Iterator over results of a query. Use 'next' to iterate or 'rest' to get all results. A cursor is closed when it is explicitly closed, all results have been read from it, garbage collected, or not used for over 10 minutes (unless 'NoCursorTimeout' option was specified in 'Query'). Reading from a closed cursor raises a ServerFailure exception. Note, a cursor is not closed when the connection is closed, so you can open another connection to the same server and continue using the cursor.

data CursorState = CS Limit CursorId [Document]
-- ^ CursorId = 0 means cursor is finished. Documents is remaining documents to serve in current batch. Limit is remaining limit for next fetch.

fromReply :: (Monad m) => Limit -> Reply -> m CursorState
fromReply limit Reply{..} = if rResponseFlag == 0
	then return (CS limit rCursorId rDocuments)
	else fail $ "Query failure " ++ show rResponseFlag ++ " " ++ show rDocuments

newCursor :: (Conn m) => Database -> Collection -> BatchSize -> CursorState -> m Cursor
-- ^ Cursor is closed when garbage collected, explicitly closed, or CIO action ends (connection closed)
newCursor db col batch cs = do
	conn <- getConnection
	var <- liftIO (newMVar cs)
	liftIO . addMVarFinalizer var $ do
		-- kill cursor on server when garbage collected on client, if connection not already closed
		CS _ cid _ <- readMVar var
		unless (cid == 0) $ do
			done <- isClosed conn
			unless done $ runTask (runOp $ P.killCursors [cid]) conn >> return ()
	return (Cursor (db <.> col) batch var)

next :: (Conn m) => Cursor -> m (Maybe Document)
-- ^ Return next document in query result, or Nothing if finished.
-- This can run inside or outside a 'Db' monad (a 'useDb' block), since @Conn m => ReaderT r m@ is an instance of the 'Conn' type class, along with @Task@ and @Op@
next (Cursor fcol batch var) = runOp . exposeIO $ \h -> modifyMVar var $ \cs ->
	-- Get lock on connection (runOp) first then get lock on cursor, otherwise you could get in deadlock if already inside an Op (connection locked), but another Task gets lock on cursor first and then tries runOp (deadlock).
	either ((cs,) . Left) (fmap Right) <$> hideIO (nextState cs) h
 where
	nextState :: CursorState -> Op (CursorState, Maybe Document)
	nextState (CS limit cid docs) = case docs of
		doc : docs' -> return (CS limit cid docs', Just doc)
		[] -> if cid == 0
			then return (CS 0 0 [], Nothing)  -- finished
			else let  -- fetch next batch from server
				(batchSize, remLimit) = batchSizeRemainingLimit batch limit
				getNextBatch = fromReply remLimit =<< P.getMore (GetMore fcol batchSize cid)
				in nextState =<< getNextBatch

nextN :: (Conn m) => Int -> Cursor -> m [Document]
-- ^ Return next N documents or less if end is reached
nextN n c = catMaybes <$> replicateM n (next c)

rest :: (Conn m) => Cursor -> m [Document]
-- ^ Return remaining documents in query result
rest c = loop (next c)

closeCursor :: (Conn m) => Cursor -> m ()
-- ^ Close cursor without reading rest of results. Cursor closes automatically when you read all results.
closeCursor (Cursor _ _ var) = runOp . exposeIO $ \h ->
	modifyMVar var $ \cs@(CS _ cid _) -> if cid == 0
		then return (CS 0 0 [], Right ())
		else either ((cs,) . Left) ((CS 0 0 [],) . Right) <$> hideIO (P.killCursors [cid]) h

-- ** Group

data Group = Group {
	gColl :: Collection,
	gKey :: GroupKey,  -- ^ Fields to group by
	gReduce :: Javascript,  -- ^ The reduce function aggregates (reduces) the objects iterated. Typical operations of a reduce function include summing and counting. reduce takes two arguments: the current document being iterated over and the aggregation value.
	gInitial :: Document,  -- ^ Initial aggregation value supplied to reduce
	gCond :: Selector,  -- ^ Condition that must be true for a row to be considered. [] means always true.
	gFinalize :: Maybe Javascript  -- ^ An optional function to be run on each item in the result set just before the item is returned. Can either modify the item (e.g., add an average field given a count and a total) or return a replacement object (returning a new object with just _id and average fields).
	} deriving (Show, Eq)

data GroupKey = Key [Label] | KeyF Javascript  deriving (Show, Eq)
-- ^ Fields to group by, or function returning a "key object" to be used as the grouping key. Use this instead of key to specify a key that is not an existing member of the object (or, to access embedded members).

groupDocument :: Group -> Document
-- ^ Translate Group data into expected document form
groupDocument Group{..} =
	("finalize" =? gFinalize) ++ [
	"ns" =: gColl,
	case gKey of Key k -> "key" =: map (=: True) k; KeyF f -> "$keyf" =: f,
	"$reduce" =: gReduce,
	"initial" =: gInitial,
	"cond" =: gCond ]

group :: (Conn m) => Group -> Db m [Document]
-- ^ Execute group query and return resulting aggregate value for each distinct key
group g = at "retval" <$> runCommand ["group" =: groupDocument g]

-- ** MapReduce

-- | Maps every document in collection to a (key, value) pair, then for each unique key reduces all its associated values to a result. Therefore, the final output is a list of (key, result) pairs, where every key is unique. This is the basic description. There are additional nuances that may be used. See <http://www.mongodb.org/display/DOCS/MapReduce> for details.
data MapReduce = MapReduce {
	rColl :: Collection,
	rMap :: MapFun,
	rReduce :: ReduceFun,
	rSelect :: Selector,  -- ^ Default is []
	rSort :: Order,  -- ^ Default is [] meaning no sort
	rLimit :: Limit,  -- ^ Default is 0 meaning no limit
	rOut :: Maybe Collection,  -- ^ Output to given permanent collection, otherwise output to a new temporary collection whose name is returned.
	rKeepTemp :: Bool,  -- ^ If True, the temporary output collection is made permanent. If False, the temporary output collection persists for the life of the current connection only, however, other connections may read from it while the original one is still alive. Note, reading from a temporary collection after its original connection dies returns an empty result (not an error). The default for this attribute is False, unless 'rOut' is specified, then the collection permanent.
	rFinalize :: Maybe FinalizeFun,  -- ^ Function to apply to all the results when finished. Default is Nothing.
	rScope :: Document,  -- ^ Variables (environment) that can be accessed from map/reduce/finalize. Default is [].
	rVerbose :: Bool  -- ^ Provide statistics on job execution time. Default is False.
	} deriving (Show, Eq)

type MapFun = Javascript
-- ^ @() -> void@. The map function references the variable this to inspect the current object under consideration. A map function must call @emit(key,value)@ at least once, but may be invoked any number of times, as may be appropriate.

type ReduceFun = Javascript
-- ^ @(key, value_array) -> value@. The reduce function receives a key and an array of values. To use, reduce the received values, and return a result. The MapReduce engine may invoke reduce functions iteratively; thus, these functions must be idempotent.  That is, the following must hold for your reduce function: @for all k, vals : reduce(k, [reduce(k,vals)]) == reduce(k,vals)@. If you need to perform an operation only once, use a finalize function. The output of emit (the 2nd param) and reduce should be the same format to make iterative reduce possible.

type FinalizeFun = Javascript
-- ^ @(key, value) -> final_value@. A finalize function may be run after reduction.  Such a function is optional and is not necessary for many map/reduce cases.  The finalize function takes a key and a value, and returns a finalized value.

mrDocument :: MapReduce -> Document
-- ^ Translate MapReduce data into expected document form
mrDocument MapReduce{..} =
	("mapreduce" =: rColl) :
	("out" =? rOut) ++
	("finalize" =? rFinalize) ++ [
	"map" =: rMap,
	"reduce" =: rReduce,
	"query" =: rSelect,
	"sort" =: rSort,
	"limit" =: (fromIntegral rLimit :: Int),
	"keeptemp" =: rKeepTemp,
	"scope" =: rScope,
	"verbose" =: rVerbose ]

mapReduce :: Collection -> MapFun -> ReduceFun -> MapReduce
-- ^ MapReduce on collection with given map and reduce functions. Remaining attributes are set to their defaults, which are stated in their comments.
mapReduce col map' red = MapReduce col map' red [] [] 0 Nothing False Nothing [] False

runMR :: (Conn m) => MapReduce -> Db m Cursor
-- ^ Run MapReduce and return cursor of results. Error if map/reduce fails (because of bad Javascript)
-- TODO: Delete temp result collection when cursor closes. Until then, it will be deleted by the server when connection closes.
runMR mr = find . query [] =<< (at "result" <$> runMR' mr)

runMR' :: (Conn m) => MapReduce -> Db m Document
-- ^ Run MapReduce and return a result document containing a "result" field holding the output Collection and additional statistic fields. Error if the map/reduce failed (because of bad Javascript).
runMR' mr = do
	doc <- runCommand (mrDocument mr)
	return $ if true1 "ok" doc then doc else error $ at "errmsg" doc ++ " in:\n" ++ show mr

-- * Command

type Command = Document
-- ^ A command is a special query or action against the database. See <http://www.mongodb.org/display/DOCS/Commands> for details.

runCommand :: (Conn m) => Command -> Db m Document
-- ^ Run command against the database and return its result
runCommand c = maybe err return =<< findOne (query c "$cmd") where
	err = fail $ "Nothing returned for command: " ++ show c

runCommand1 :: (Conn m) => UString -> Db m Document
-- ^ @runCommand1 "foo" = runCommand ["foo" =: 1]@
runCommand1 c = runCommand [c =: (1 :: Int)]

eval :: (Conn m) => Javascript -> Db m Document
-- ^ Run code on server
eval code = at "retval" <$> runCommand ["$eval" =: code]

type ErrorCode = Int
-- ^ Error code from getLastError

getLastError :: Db Op (Maybe (ErrorCode, String))
-- ^ Fetch what the last error was, Nothing means no error. Especially useful after a write since it is asynchronous (ie. nothing is returned after a write, so we don't know if it succeeded or not). To ensure no interleaving db operation executes between the write we want to check and getLastError, this can only be executed inside a 'runDbOp' which gets exclusive access to the connection.
getLastError = do
	r <- runCommand1 "getlasterror"
	return $ (at "code" r,) <$> lookup "err" r

resetLastError :: Db Op ()
-- ^ Clear last error
resetLastError = runCommand1 "reseterror" >> return ()


{- Authors: Tony Hannan <tony@10gen.com>
   Copyright 2010 10gen Inc.
   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. -}
