-- | Query and update documents residing on a MongoDB server(s)

{-# LANGUAGE OverloadedStrings, RecordWildCards, NamedFieldPuns, TupleSections, FlexibleContexts, FlexibleInstances, UndecidableInstances, MultiParamTypeClasses #-}

module Database.MongoDB.Query (
	-- * Connection
	Failure(..), Conn, Connected, runConn,
	-- * Database
	Database, allDatabases, DbConn, useDb, thisDatabase,
	-- ** Authentication
	P.Username, P.Password, auth,
	-- * Collection
	Collection, allCollections,
	-- ** Selection
	Selection(..), Selector, whereJS,
	Select(select),
	-- * Write
	-- ** WriteMode
	WriteMode(..), writeMode,
	-- ** Insert
	insert, insert_, insertMany, insertMany_,
	-- ** Update
	save, replace, repsert, Modifier, modify,
	-- ** Delete
	delete, deleteOne,
	-- * Read
	-- ** Query
	Query(..), P.QueryOption(..), Projector, Limit, Order, BatchSize,
	explain, find, findOne, count, distinct,
	-- *** Cursor
	Cursor, next, nextN, rest,
	-- ** Group
	Group(..), GroupKey(..), group,
	-- ** MapReduce
	MapReduce(..), MapFun, ReduceFun, FinalizeFun, mapReduce, runMR, runMR',
	-- * Command
	Command, runCommand, runCommand1,
	eval,
) where

import Prelude as X hiding (lookup)
import Control.Applicative ((<$>), Applicative(..))
import Control.Arrow (left, first, second)
import Control.Monad.Context
import Control.Monad.Reader
import Control.Monad.Error
import System.IO.Error (try)
import Control.Concurrent.MVar
import Control.Pipeline (Resource(..))
import qualified Database.MongoDB.Internal.Protocol as P
import Database.MongoDB.Internal.Protocol hiding (Query, send, call)
import Data.Bson
import Data.Word
import Data.Int
import Data.Maybe (listToMaybe, catMaybes)
import Data.UString as U (dropWhile, any, tail)
import Database.MongoDB.Internal.Util (loop, (<.>), true1)  -- plus Applicative instances of ErrorT & ReaderT

-- * Connection

-- | A monad with access to a 'Connection' and 'WriteMode' and throws a 'Failure' on connection or server failure
class (Context Connection m, Context WriteMode m, MonadError Failure m, MonadIO m, Applicative m, Functor m) => Conn m
instance (Context Connection m, Context WriteMode m, MonadError Failure m, MonadIO m, Applicative m, Functor m) => Conn m

-- | Connection or Server failure like network problem or disk full
data Failure =
	ConnectionFailure IOError
	-- ^ Error during sending or receiving bytes over a 'Connection'. The connection is not automatically closed when this error happens; the user must close it. Any other IOErrors raised during a Task or Op are not caught. The user is responsible for these other types of errors not related to sending/receiving bytes over the connection.
	| ServerFailure String
	-- ^ Failure on server, like disk full, which is usually observed using getLastError. Calling 'fail' inside a connected monad raises this failure. Do not call 'fail' unless it is a temporary server failure, like disk full. For example, receiving unexpected data from the server is not a server failure, rather it is a programming error (you should call 'error' in this case) because the client and server are incompatible and requires a programming change.
	deriving (Show, Eq)

instance Error Failure where strMsg = ServerFailure

type Connected m = ErrorT Failure (ReaderT WriteMode (ReaderT Connection m))

runConn :: Connected m a -> Connection -> m (Either Failure a)
-- ^ Run action with access to connection. Return Left Failure if connection or server fails during execution.
runConn action = runReaderT (runReaderT (runErrorT action) Unsafe)

send :: (Conn m) => [Notice] -> m ()
-- ^ Send notices as a contiguous batch to server with no reply. Raise Failure if connection fails.
send ns = do
	conn <- context
	e <- liftIO $ try (P.send conn ns)
	either (throwError . ConnectionFailure) return e

call :: (Conn m) => [Notice] -> Request -> m (IO (Either Failure Reply))
-- ^ Send notices and request as a contiguous batch to server and return reply promise, which will block when invoked until reply arrives. This call will raise Failure if connection fails send, and promise will return Failure if connection fails receive.
call ns r = do
	conn <- context
	e <- liftIO $ try (P.call conn ns r)
	case e of
		Left err -> throwError (ConnectionFailure err)
		Right promise -> return (left ConnectionFailure <$> try promise)

-- * Database

type Database = UString
-- ^ Database name

-- | A 'Conn' monad with access to a 'Database'
class (Context Database m, Conn m) => DbConn m
instance (Context Database m, Conn m) => DbConn m

allDatabases :: (Conn m) => m [Database]
-- ^ List all databases residing on server
allDatabases = map (at "name") . at "databases" <$> useDb "admin" (runCommand1 "listDatabases")

useDb :: Database -> ReaderT Database m a -> m a
-- ^ Run Db action against given database
useDb = flip runReaderT

thisDatabase :: (DbConn m) => m Database
-- ^ Current database in use
thisDatabase = context

-- * Authentication

auth :: (DbConn m) => Username -> Password -> m Bool
-- ^ Authenticate with the database (if server is running in secure mode). Return whether authentication was successful or not. Reauthentication is required for every new connection.
auth u p = do
	n <- at "nonce" <$> runCommand ["getnonce" =: (1 :: Int)]
	true1 "ok" <$> runCommand ["authenticate" =: (1 :: Int), "user" =: u, "nonce" =: n, "key" =: pwKey n u p]

-- * Collection

type Collection = UString
-- ^ Collection name (not prefixed with database)

allCollections :: (DbConn m) => m [Collection]
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

{-select :: Selector -> Collection -> Selection
-- ^ Synonym for 'Select'
select = Select-}

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
	| Safe  -- ^ Receive an acknowledgment after every write, and raise exception if one says the write failed.
	deriving (Show, Eq)

writeMode :: (Conn m) => WriteMode -> m a -> m a
-- ^ Run action with given 'WriteMode'
writeMode = push . const

write :: (DbConn m) => Notice -> m ()
-- ^ Send write to server, and if write-mode is 'Safe' then include getLastError request and raise 'ServerFailure' if it reports an error.
write notice = do
	mode <- context
	case mode of
		Unsafe -> send [notice]
		Safe -> do
			me <- getLastError [notice]
			maybe (return ()) (throwError . ServerFailure . show) me

type ErrorCode = Int
-- ^ Error code from getLastError

getLastError :: (DbConn m) => [Notice] -> m (Maybe (ErrorCode, String))
-- ^ Send notices (writes) then fetch what the last error was, Nothing means no error
getLastError writes = do
	r <- runCommand' writes ["getlasterror" =: (1 :: Int)]
	return $ (at "code" r,) <$> lookup "err" r

{-resetLastError :: (DbConn m) => m ()
-- ^ Clear last error
resetLastError = runCommand1 "reseterror" >> return ()-}

-- ** Insert

insert :: (DbConn m) => Collection -> Document -> m Value
-- ^ Insert document into collection and return its \"_id\" value, which is created automatically if not supplied
insert col doc = head <$> insertMany col [doc]

insert_ :: (DbConn m) => Collection -> Document -> m ()
-- ^ Same as 'insert' except don't return _id
insert_ col doc = insert col doc >> return ()

insertMany :: (DbConn m) => Collection -> [Document] -> m [Value]
-- ^ Insert documents into collection and return their \"_id\" values, which are created automatically if not supplied
insertMany col docs = do
	db <- thisDatabase
	docs' <- liftIO $ mapM assignId docs
	write (Insert (db <.> col) docs')
	mapM (look "_id") docs'

insertMany_ :: (DbConn m) => Collection -> [Document] -> m ()
-- ^ Same as 'insertMany' except don't return _ids
insertMany_ col docs = insertMany col docs >> return ()

assignId :: Document -> IO Document
-- ^ Assign a unique value to _id field if missing
assignId doc = if X.any (("_id" ==) . label) doc
	then return doc
	else (\oid -> ("_id" =: oid) : doc) <$> genObjectId

-- ** Update 

save :: (DbConn m) => Collection -> Document -> m ()
-- ^ Save document to collection, meaning insert it if its new (has no \"_id\" field) or update it if its not new (has \"_id\" field)
save col doc = case look "_id" doc of
	Nothing -> insert_ col doc
	Just i -> repsert (Select ["_id" := i] col) doc

replace :: (DbConn m) => Selection -> Document -> m ()
-- ^ Replace first document in selection with given document
replace = update []

repsert :: (DbConn m) => Selection -> Document -> m ()
-- ^ Replace first document in selection with given document, or insert document if selection is empty
repsert = update [Upsert]

type Modifier = Document
-- ^ Update operations on fields in a document. See <http://www.mongodb.org/display/DOCS/Updating#Updating-ModifierOperations>

modify :: (DbConn m) => Selection -> Modifier -> m ()
-- ^ Update all documents in selection using given modifier
modify = update [MultiUpdate]

update :: (DbConn m) => [UpdateOption] -> Selection -> Document -> m ()
-- ^ Update first document in selection using updater document, unless 'MultiUpdate' option is supplied then update all documents in selection. If 'Upsert' option is supplied then treat updater as document and insert it if selection is empty.
update opts (Select sel col) up = do
	db <- thisDatabase
	write (Update (db <.> col) opts sel up)

-- ** Delete

delete :: (DbConn m) => Selection -> m ()
-- ^ Delete all documents in selection
delete = delete' []

deleteOne :: (DbConn m) => Selection -> m ()
-- ^ Delete first document in selection
deleteOne = delete' [SingleRemove]

delete' :: (DbConn m) => [DeleteOption] -> Selection -> m ()
-- ^ Delete all documents in selection unless 'SingleRemove' option is given then only delete first document in selection
delete' opts (Select sel col) = do
	db <- thisDatabase
	write (Delete (db <.> col) opts sel)

-- * Read

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

queryRequest :: Bool -> Query -> Database -> (Request, Limit)
-- ^ Translate Query to Protocol.Query. If first arg is true then add special $explain attribute.
queryRequest isExplain Query{..} db = (P.Query{..}, remainingLimit) where
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

runQuery :: (DbConn m) => Bool -> [Notice] -> Query -> m CursorState'
-- ^ Send query request and return cursor state
runQuery isExplain ns q = call' ns . queryRequest isExplain q =<< thisDatabase

find :: (DbConn m) => Query -> m Cursor
-- ^ Fetch documents satisfying query
find q@Query{selection, batchSize} = do
	db <- thisDatabase
	cs' <- runQuery False [] q
	newCursor db (coll selection) batchSize cs'

findOne' :: (DbConn m) => [Notice] -> Query -> m (Maybe Document)
-- ^ Send notices and fetch first document satisfying query or Nothing if none satisfy it
findOne' ns q = do
	CS _ _ docs <- cursorState =<< runQuery False ns q {limit = 1}
	return (listToMaybe docs)

findOne :: (DbConn m) => Query -> m (Maybe Document)
-- ^ Fetch first document satisfying query or Nothing if none satisfy it
findOne = findOne' []

explain :: (DbConn m) => Query -> m Document
-- ^ Return performance stats of query execution
explain q = do  -- same as findOne but with explain set to true
	CS _ _ docs <- cursorState =<< runQuery True [] q {limit = 1}
	return $ if null docs then error ("no explain: " ++ show q) else head docs

count :: (DbConn m) => Query -> m Int
-- ^ Fetch number of documents satisfying query (including effect of skip and/or limit if present)
count Query{selection = Select sel col, skip, limit} = at "n" <$> runCommand
	(["count" =: col, "query" =: sel, "skip" =: (fromIntegral skip :: Int32)]
		++ ("limit" =? if limit == 0 then Nothing else Just (fromIntegral limit :: Int32)))

distinct :: (DbConn m) => Label -> Selection -> m [Value]
-- ^ Fetch distinct values of field in selected documents
distinct k (Select sel col) = at "values" <$> runCommand ["distinct" =: col, "key" =: k, "query" =: sel]

-- *** Cursor

data Cursor = Cursor FullCollection BatchSize (MVar CursorState')
-- ^ Iterator over results of a query. Use 'next' to iterate or 'rest' to get all results. A cursor is closed when it is explicitly closed, all results have been read from it, garbage collected, or not used for over 10 minutes (unless 'NoCursorTimeout' option was specified in 'Query'). Reading from a closed cursor raises a ServerFailure exception. Note, a cursor is not closed when the connection is closed, so you can open another connection to the same server and continue using the cursor.

modifyCursorState' :: (Conn m) => Cursor -> (FullCollection -> BatchSize -> CursorState' -> Connected IO (CursorState', a)) -> m a
-- ^ Analogous to 'modifyMVar' but with Conn monad
modifyCursorState' (Cursor fcol batch var) act = do
	conn <- context
	e <- liftIO . modifyMVar var $ \cs' ->
		either ((cs',) . Left) (second Right) <$> runConn (act fcol batch cs') conn
	either throwError return e

getCursorState :: (Conn m) => Cursor -> m CursorState
-- ^ Extract current cursor status
getCursorState (Cursor _ _ var) = cursorState =<< liftIO (readMVar var)

data CursorState' = Delayed (IO (Either Failure CursorState)) | CursorState CursorState
-- ^ A cursor state or a promised cursor state which may fail

cursorState :: (Conn m) => CursorState' -> m CursorState
-- ^ Convert promised cursor state to cursor state or raise Failure
cursorState (Delayed promise) = either throwError return =<< liftIO promise
cursorState (CursorState cs) = return cs

data CursorState = CS Limit CursorId [Document]
-- ^ CursorId = 0 means cursor is finished. Documents is remaining documents to serve in current batch. Limit is remaining limit for next fetch.

fromReply :: Limit -> Reply -> Either Failure CursorState
-- ^ Convert Reply to CursorState or Failure
fromReply limit Reply{..} = if rResponseFlag == 0
	then Right (CS limit rCursorId rDocuments)
	else Left . ServerFailure $ "Query failure " ++ show rResponseFlag ++ " " ++ show rDocuments

call' :: (Conn m) => [Notice] -> (Request, Limit) -> m CursorState'
-- ^ Send notices and request and return promised cursor state
call' ns (req, remainingLimit) = do
	promise <- call ns req
	return $ Delayed (fmap (fromReply remainingLimit =<<) promise)

newCursor :: (Conn m) => Database -> Collection -> BatchSize -> CursorState' -> m Cursor
-- ^ Create new cursor. If you don't read all results then close it. Cursor will be closed automatically when all results are read from it or when eventually garbage collected.
newCursor db col batch cs = do
	conn <- context
	var <- liftIO (newMVar cs)
	let cursor = Cursor (db <.> col) batch var
	liftIO . addMVarFinalizer var $ runConn (close cursor) conn >> return ()
	return cursor

next :: (Conn m) => Cursor -> m (Maybe Document)
-- ^ Return next document in query result, or Nothing if finished.
next cursor = modifyCursorState' cursor nextState where
	-- Pre-fetch next batch promise from server when last one in current batch is returned.
	nextState :: FullCollection -> BatchSize -> CursorState' -> Connected IO (CursorState', Maybe Document)
	nextState fcol batch cs' = do
		CS limit cid docs <- cursorState cs'
		case docs of
			doc : docs' -> do
				cs'' <- if null docs' && cid /= 0
					then nextBatch fcol batch limit cid
					else return $ CursorState (CS limit cid docs')
				return (cs'', Just doc)
			[] -> if cid == 0
				then return (CursorState $ CS 0 0 [], Nothing)  -- finished
				else error $ "server returned empty batch but says more results on server"
	nextBatch fcol batch limit cid = let
		(batchSize, remLimit) = batchSizeRemainingLimit batch limit
		in call' [] (GetMore fcol batchSize cid, remLimit)

nextN :: (Conn m) => Int -> Cursor -> m [Document]
-- ^ Return next N documents or less if end is reached
nextN n c = catMaybes <$> replicateM n (next c)

rest :: (Conn m) => Cursor -> m [Document]
-- ^ Return remaining documents in query result
rest c = loop (next c)

instance (Conn m) => Resource m Cursor where
	close cursor = modifyCursorState' cursor kill' where
 		kill' _ _ cs' = first CursorState <$> (kill =<< cursorState cs')
		kill (CS _ cid _) = (CS 0 0 [],) <$> if cid == 0 then return () else send [KillCursors [cid]]
	isClosed cursor = do
		CS _ cid docs <- getCursorState cursor
		return (cid == 0 && null docs)

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

group :: (DbConn m) => Group -> m [Document]
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

runMR :: (DbConn m) => MapReduce -> m Cursor
-- ^ Run MapReduce and return cursor of results. Error if map/reduce fails (because of bad Javascript)
-- TODO: Delete temp result collection when cursor closes. Until then, it will be deleted by the server when connection closes.
runMR mr = find . query [] =<< (at "result" <$> runMR' mr)

runMR' :: (DbConn m) => MapReduce -> m Document
-- ^ Run MapReduce and return a result document containing a "result" field holding the output Collection and additional statistic fields. Error if the map/reduce failed (because of bad Javascript).
runMR' mr = do
	doc <- runCommand (mrDocument mr)
	return $ if true1 "ok" doc then doc else error $ at "errmsg" doc ++ " in:\n" ++ show mr

-- * Command

type Command = Document
-- ^ A command is a special query or action against the database. See <http://www.mongodb.org/display/DOCS/Commands> for details.

runCommand' :: (DbConn m) => [Notice] -> Command -> m Document
-- ^ Send notices then run command and return its result
runCommand' ns c = maybe err id <$> findOne' ns (query c "$cmd") where
	err = error $ "Nothing returned for command: " ++ show c

runCommand :: (DbConn m) => Command -> m Document
-- ^ Run command against the database and return its result
runCommand = runCommand' []

runCommand1 :: (DbConn m) => UString -> m Document
-- ^ @runCommand1 foo = runCommand [foo =: 1]@
runCommand1 c = runCommand [c =: (1 :: Int)]

eval :: (DbConn m) => Javascript -> m Document
-- ^ Run code on server
eval code = at "retval" <$> runCommand ["$eval" =: code]


{- Authors: Tony Hannan <tony@10gen.com>
   Copyright 2010 10gen Inc.
   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. -}
