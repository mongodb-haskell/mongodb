-- | Database administrative functions

{-# LANGUAGE OverloadedStrings, RecordWildCards #-}

module Database.MongoDB.Admin (
	-- * Admin
	-- ** Collection
	CollectionOption(..), createCollection, renameCollection, dropCollection, validateCollection,
	-- ** Index
	Index(..), IndexName, index, ensureIndex, createIndex, dropIndex, getIndexes, dropIndexes,
	-- ** User
	allUsers, addUser, removeUser,
	-- ** Database
	admin, cloneDatabase, copyDatabase, dropDatabase, repairDatabase,
	-- ** Server
	serverBuildInfo, serverVersion,
	-- * Diagnotics
	-- ** Collection
	collectionStats, dataSize, storageSize, totalIndexSize, totalSize,
	-- ** Profiling
	ProfilingLevel, getProfilingLevel, MilliSec, setProfilingLevel,
	-- ** Database
	dbStats, OpNum, currentOp, killOp,
	-- ** Server
	serverStatus
) where

import Prelude hiding (lookup)
import Control.Applicative ((<$>))
import Database.MongoDB.Internal.Protocol (pwHash, pwKey)
import Database.MongoDB.Connection (Host, showHostPort)
import Database.MongoDB.Query
import Data.Bson
import Data.UString (pack, unpack, append, intercalate)
import Control.Monad.Reader
import qualified Data.HashTable as T
import Data.IORef
import qualified Data.Set as S
import System.IO.Unsafe (unsafePerformIO)
import Control.Concurrent (forkIO, threadDelay)
import Database.MongoDB.Internal.Util ((<.>), true1)

-- * Admin

-- ** Collection

data CollectionOption = Capped | MaxByteSize Int | MaxItems Int  deriving (Show, Eq)

coptElem :: CollectionOption -> Field
coptElem Capped = "capped" =: True
coptElem (MaxByteSize n) = "size" =: n
coptElem (MaxItems n) = "max" =: n

createCollection :: (DbAccess m) => [CollectionOption] -> Collection -> m Document
-- ^ Create collection with given options. You only need to call this to set options, otherwise a collection is created automatically on first use with no options.
createCollection opts col = runCommand $ ["create" =: col] ++ map coptElem opts

renameCollection :: (DbAccess m) => Collection -> Collection -> m Document
-- ^ Rename first collection to second collection
renameCollection from to = do
	Database db <- thisDatabase
	use admin $ runCommand ["renameCollection" =: db <.> from, "to" =: db <.> to, "dropTarget" =: True]

dropCollection :: (DbAccess m) => Collection -> m Bool
-- ^ Delete the given collection! Return True if collection existed (and was deleted); return False if collection did not exist (and no action).
dropCollection coll = do
	resetIndexCache
	r <- runCommand ["drop" =: coll]
	if true1 "ok" r then return True else do
		if at "errmsg" r == ("ns not found" :: UString) then return False else
			fail $ "dropCollection failed: " ++ show r

validateCollection :: (DbAccess m) => Collection -> m Document
-- ^ This operation takes a while
validateCollection coll = runCommand ["validate" =: coll]

-- ** Index

type IndexName = UString

data Index = Index {
	iColl :: Collection,
	iKey :: Order,
	iName :: IndexName,
	iUnique :: Bool,
	iDropDups :: Bool
	} deriving (Show, Eq)

idxDocument :: Index -> Database -> Document
idxDocument Index{..} (Database db) = [
	"ns" =: db <.> iColl,
	"key" =: iKey,
	"name" =: iName,
	"unique" =: iUnique,
	"dropDups" =: iDropDups ]

index :: Collection -> Order -> Index
-- ^ Spec of index of ordered keys on collection. Name is generated from keys. Unique and dropDups are False.
index coll keys = Index coll keys (genName keys) False False

genName :: Order -> IndexName
genName keys = intercalate "_" (map f keys)  where
	f (k := v) = k `append` "_" `append` pack (show v)

ensureIndex :: (DbAccess m) => Index -> m ()
-- ^ Create index if we did not already create one. May be called repeatedly with practically no performance hit, because we remember if we already called this for the same index (although this memory gets wiped out every 15 minutes, in case another client drops the index and we want to create it again).
ensureIndex idx = let k = (iColl idx, iName idx) in do
	icache <- fetchIndexCache
	set <- liftIO (readIORef icache)
	unless (S.member k set) $ do
		writeMode (Safe []) (createIndex idx)
		liftIO $ writeIORef icache (S.insert k set)

createIndex :: (DbAccess m) => Index -> m ()
-- ^ Create index on the server. This call goes to the server every time.
createIndex idx = insert_ "system.indexes" . idxDocument idx =<< thisDatabase

dropIndex :: (DbAccess m) => Collection -> IndexName -> m Document
-- ^ Remove the index
dropIndex coll idxName = do
	resetIndexCache
	runCommand ["deleteIndexes" =: coll, "index" =: idxName]

getIndexes :: (DbAccess m) => Collection -> m [Document]
-- ^ Get all indexes on this collection
getIndexes coll = do
	Database db <- thisDatabase
	rest =<< find (select ["ns" =: db <.> coll] "system.indexes")

dropIndexes :: (DbAccess m) => Collection -> m Document
-- ^ Drop all indexes on this collection
dropIndexes coll = do
	resetIndexCache
	runCommand ["deleteIndexes" =: coll, "index" =: ("*" :: UString)]

-- *** Index cache

type DbIndexCache = T.HashTable Database IndexCache
-- ^ Cache the indexes we create so repeatedly calling ensureIndex only hits database the first time. Clear cache every once in a while so if someone else deletes index we will recreate it on ensureIndex.

type IndexCache = IORef (S.Set (Collection, IndexName))

dbIndexCache :: DbIndexCache
-- ^ initialize cache and fork thread that clears it every 15 minutes
dbIndexCache = unsafePerformIO $ do
	table <- T.new (==) (T.hashString . unpack . databaseName)
	_ <- forkIO . forever $ threadDelay 900000000 >> clearDbIndexCache
	return table
{-# NOINLINE dbIndexCache #-}

clearDbIndexCache :: IO ()
clearDbIndexCache = do
	keys <- map fst <$> T.toList dbIndexCache
	mapM_ (T.delete dbIndexCache) keys

fetchIndexCache :: (DbAccess m) => m IndexCache
-- ^ Get index cache for current database
fetchIndexCache = do
	db <- thisDatabase
	liftIO $ do
		mc <- T.lookup dbIndexCache db
		maybe (newIdxCache db) return mc
 where
	newIdxCache db = do
		idx <- newIORef S.empty
		T.insert dbIndexCache db idx
		return idx

resetIndexCache :: (DbAccess m) => m ()
-- ^ reset index cache for current database
resetIndexCache = do
	icache <- fetchIndexCache
	liftIO (writeIORef icache S.empty)

-- ** User

allUsers :: (DbAccess m) => m [Document]
-- ^ Fetch all users of this database
allUsers = map (exclude ["_id"]) <$> (rest =<< find
	(select [] "system.users") {sort = ["user" =: (1 :: Int)], project = ["user" =: (1 :: Int), "readOnly" =: (1 :: Int)]})

addUser :: (DbAccess m) => Bool -> Username -> Password -> m ()
-- ^ Add user with password with read-only access if bool is True or read-write access if bool is False
addUser readOnly user pass = do
	mu <- findOne (select ["user" =: user] "system.users")
	let u = merge ["readOnly" =: readOnly, "pwd" =: pwHash user pass] (maybe ["user" =: user] id mu)
	save "system.users" u

removeUser :: (DbAccess m) => Username -> m ()
removeUser user = delete (select ["user" =: user] "system.users")

-- ** Database

admin :: Database
-- ^ \"admin\" database
admin = Database "admin"

cloneDatabase :: (Access m) => Database -> Host -> m Document
-- ^ Copy database from given host to the server I am connected to. Fails and returns @"ok" = 0@ if we don't have permission to read from given server (use copyDatabase in this case).
cloneDatabase db fromHost = use db $ runCommand ["clone" =: showHostPort fromHost]

copyDatabase :: (Access m) => Database -> Host -> Maybe (Username, Password) -> Database -> m Document
-- ^ Copy database from given host to the server I am connected to. If username & password is supplied use them to read from given host.
copyDatabase (Database fromDb) fromHost mup (Database toDb) = do
	let c = ["copydb" =: (1 :: Int), "fromhost" =: showHostPort fromHost, "fromdb" =: fromDb, "todb" =: toDb]
	use admin $ case mup of
		Nothing -> runCommand c
		Just (u, p) -> do
			n <- at "nonce" <$> runCommand ["copydbgetnonce" =: (1 :: Int), "fromhost" =: showHostPort fromHost]
			runCommand $ c ++ ["username" =: u, "nonce" =: n, "key" =: pwKey n u p]

dropDatabase :: (Access m) => Database -> m Document
-- ^ Delete the given database!
dropDatabase db = use db $ runCommand ["dropDatabase" =: (1 :: Int)]

repairDatabase :: (Access m) => Database -> m Document
-- ^ Attempt to fix any corrupt records. This operation takes a while.
repairDatabase db = use db $ runCommand ["repairDatabase" =: (1 :: Int)]

-- ** Server

serverBuildInfo :: (Access m) => m Document
serverBuildInfo = use admin $ runCommand ["buildinfo" =: (1 :: Int)]

serverVersion :: (Access m) => m UString
serverVersion = at "version" <$> serverBuildInfo

-- * Diagnostics

-- ** Collection

collectionStats :: (DbAccess m) => Collection -> m Document
collectionStats coll = runCommand ["collstats" =: coll]

dataSize :: (DbAccess m) => Collection -> m Int
dataSize c = at "size" <$> collectionStats c

storageSize :: (DbAccess m) => Collection -> m Int
storageSize c = at "storageSize" <$> collectionStats c

totalIndexSize :: (DbAccess m) => Collection -> m Int
totalIndexSize c = at "totalIndexSize" <$> collectionStats c

totalSize :: (DbAccess m) => Collection -> m Int
totalSize coll = do
	x <- storageSize coll
	xs <- mapM isize =<< getIndexes coll
	return (foldl (+) x xs)
 where
	isize idx = at "storageSize" <$> collectionStats (coll `append` ".$" `append` at "name" idx)

-- ** Profiling

data ProfilingLevel = Off | Slow | All  deriving (Show, Enum, Eq)

getProfilingLevel :: (DbAccess m) => m ProfilingLevel
getProfilingLevel = toEnum . at "was" <$> runCommand ["profile" =: (-1 :: Int)]

type MilliSec = Int

setProfilingLevel :: (DbAccess m) => ProfilingLevel -> Maybe MilliSec -> m ()
setProfilingLevel p mSlowMs =
	runCommand (["profile" =: fromEnum p] ++ ("slowms" =? mSlowMs)) >> return ()

-- ** Database

dbStats :: (DbAccess m) => m Document
dbStats = runCommand ["dbstats" =: (1 :: Int)]

currentOp :: (DbAccess m) => m (Maybe Document)
-- ^ See currently running operation on the database, if any
currentOp = findOne (select [] "$cmd.sys.inprog")

type OpNum = Int

killOp :: (DbAccess m) => OpNum -> m (Maybe Document)
killOp op = findOne (select ["op" =: op] "$cmd.sys.killop")

-- ** Server

serverStatus :: (Access m) => m Document
serverStatus = use admin $ runCommand ["serverStatus" =: (1 :: Int)]


{- Authors: Tony Hannan <tony@10gen.com>
   Copyright 2010 10gen Inc.
   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. -}
