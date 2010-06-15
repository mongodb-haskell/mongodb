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
	cloneDatabase, copyDatabase, dropDatabase, repairDatabase,
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
import Database.MongoDB.Connection (Server, showHostPort, Conn)
import Database.MongoDB.Query
import Data.Bson
import Data.UString (pack, unpack, append, intercalate)
import Control.Monad.Reader
import qualified Data.HashTable as T
import Data.IORef
import qualified Data.Set as S
import System.IO.Unsafe (unsafePerformIO)
import Control.Concurrent (forkIO, threadDelay)
import Database.MongoDB.Util ((<.>), true1)

-- * Admin

-- ** Collection

data CollectionOption = Capped | MaxByteSize Int | MaxItems Int  deriving (Show, Eq)

coptElem :: CollectionOption -> Field
coptElem Capped = "capped" =: True
coptElem (MaxByteSize n) = "size" =: n
coptElem (MaxItems n) = "max" =: n

createCollection :: (Conn m) => [CollectionOption] -> Collection -> Db m Document
-- ^ Create collection with given options. You only need to call this to set options, otherwise a collection is created automatically on first use with no options.
createCollection opts col = runCommand $ ["create" =: col] ++ map coptElem opts

renameCollection :: (Conn m) => Collection -> Collection -> Db m Document
-- ^ Rename first collection to second collection
renameCollection from to = ReaderT $ \db -> useDb "admin" $
	runCommand ["renameCollection" =: db <.> from, "to" =: db <.> to, "dropTarget" =: True]

dropCollection :: (Conn m) => Collection -> Db m Bool
-- ^ Delete the given collection! Return True if collection existed (and was deleted); return False if collection did not exist (and no action).
dropCollection coll = do
	resetIndexCache
	r <- runCommand ["drop" =: coll]
	if true1 "ok" r then return True else do
		if at "errmsg" r == ("ns not found" :: UString) then return False else
			fail $ "dropCollection failed: " ++ show r

validateCollection :: (Conn m) => Collection -> Db m Document
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
idxDocument Index{..} db = [
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

ensureIndex :: (Conn m) => Index -> Db m ()
-- ^ Create index if we did not already create one. May be called repeatedly with practically no performance hit, because we remember if we already called this for the same index (although this memory gets wiped out every 15 minutes, in case another client drops the index and we want to create it again).
ensureIndex idx = let k = (iColl idx, iName idx) in do
	icache <- fetchIndexCache
	set <- liftIO (readIORef icache)
	unless (S.member k set) . runDbOp $ do
		createIndex idx
		me <- getLastError
		case me of
			Nothing -> liftIO $ writeIORef icache (S.insert k set)
			Just (c, e) -> fail $ "createIndex failed: (" ++ show c ++ ") " ++ e

createIndex :: (Conn m) => Index -> Db m ()
-- ^ Create index on the server. This call goes to the server every time.
createIndex idx = insert_ "system.indexes" . idxDocument idx =<< thisDatabase

dropIndex :: (Conn m) => Collection -> IndexName -> Db m Document
-- ^ Remove the index
dropIndex coll idxName = do
	resetIndexCache
	runCommand ["deleteIndexes" =: coll, "index" =: idxName]

getIndexes :: (Conn m) => Collection -> Db m [Document]
-- ^ Get all indexes on this collection
getIndexes coll = do
	db <- thisDatabase
	rest =<< find (query ["ns" =: db <.> coll] "system.indexes")

dropIndexes :: (Conn m) => Collection -> Db m Document
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
	table <- T.new (==) (T.hashString . unpack)
	_ <- forkIO . forever $ threadDelay 900000000 >> clearDbIndexCache
	return table
{-# NOINLINE dbIndexCache #-}

clearDbIndexCache :: IO ()
clearDbIndexCache = do
	keys <- map fst <$> T.toList dbIndexCache
	mapM_ (T.delete dbIndexCache) keys

fetchIndexCache :: (Conn m) => Db m IndexCache
-- ^ Get index cache for current database
fetchIndexCache = ReaderT $ \db -> liftIO $ do
	mc <- T.lookup dbIndexCache db
	maybe (newIdxCache db) return mc
 where
	newIdxCache db = do
		idx <- newIORef S.empty
		T.insert dbIndexCache db idx
		return idx

resetIndexCache :: (Conn m) => Db m ()
-- ^ reset index cache for current database
resetIndexCache = do
	icache <- fetchIndexCache
	liftIO (writeIORef icache S.empty)

-- ** User

allUsers :: (Conn m) => Db m [Document]
-- ^ Fetch all users of this database
allUsers = map (exclude ["_id"]) <$> (rest =<< find
	(query [] "system.users") {sort = ["user" =: (1 :: Int)], project = ["user" =: (1 :: Int), "readOnly" =: (1 :: Int)]})

addUser :: (Conn m) => Bool -> Username -> Password -> Db m ()
-- ^ Add user with password with read-only access if bool is True or read-write access if bool is False
addUser readOnly user pass = do
	mu <- findOne (query ["user" =: user] "system.users")
	let u = merge ["readOnly" =: readOnly, "pwd" =: pwHash user pass] (maybe ["user" =: user] id mu)
	save "system.users" u

removeUser :: (Conn m) => Username -> Db m ()
removeUser user = delete (Select ["user" =: user] "system.users")

-- ** Database

cloneDatabase :: (Conn m) => Database -> Server -> m Document
-- ^ Copy database from given server to the server I am connected to. Fails and returns @"ok" = 0@ if we don't have permission to read from given server (use copyDatabase in this case).
cloneDatabase db fromHost = useDb db $ runCommand ["clone" =: showHostPort fromHost]

copyDatabase :: (Conn m) => Database -> Server -> Maybe (Username, Password) -> Database -> m Document
-- ^ Copy database from given server to the server I am connected to. If username & password is supplied use them to read from given server.
copyDatabase fromDb fromHost mup toDb = do
	let c = ["copydb" =: (1 :: Int), "fromhost" =: showHostPort fromHost, "fromdb" =: fromDb, "todb" =: toDb]
	useDb "admin" $ case mup of
		Nothing -> runCommand c
		Just (u, p) -> do
			n <- at "nonce" <$> runCommand ["copydbgetnonce" =: (1 :: Int), "fromhost" =: showHostPort fromHost]
			runCommand $ c ++ ["username" =: u, "nonce" =: n, "key" =: pwKey n u p]

dropDatabase :: (Conn m) => Database -> m Document
-- ^ Delete the given database!
dropDatabase db = useDb db $ runCommand ["dropDatabase" =: (1 :: Int)]

repairDatabase :: (Conn m) => Database -> m Document
-- ^ Attempt to fix any corrupt records. This operation takes a while.
repairDatabase db = useDb db $ runCommand ["repairDatabase" =: (1 :: Int)]

-- ** Server

serverBuildInfo :: (Conn m) => m Document
serverBuildInfo = useDb "admin" $ runCommand ["buildinfo" =: (1 :: Int)]

serverVersion :: (Conn m) => m UString
serverVersion = at "version" <$> serverBuildInfo

-- * Diagnostics

-- ** Collection

collectionStats :: (Conn m) => Collection -> Db m Document
collectionStats coll = runCommand ["collstats" =: coll]

dataSize :: (Conn m) => Collection -> Db m Int
dataSize c = at "size" <$> collectionStats c

storageSize :: (Conn m) => Collection -> Db m Int
storageSize c = at "storageSize" <$> collectionStats c

totalIndexSize :: (Conn m) => Collection -> Db m Int
totalIndexSize c = at "totalIndexSize" <$> collectionStats c

totalSize :: (Conn m) => Collection -> Db m Int
totalSize coll = do
	x <- storageSize coll
	xs <- mapM isize =<< getIndexes coll
	return (foldl (+) x xs)
 where
	isize idx = at "storageSize" <$> collectionStats (coll `append` ".$" `append` at "name" idx)

-- ** Profiling

data ProfilingLevel = Off | Slow | All  deriving (Show, Enum, Eq)

getProfilingLevel :: (Conn m) => Db m ProfilingLevel
getProfilingLevel = toEnum . at "was" <$> runCommand ["profile" =: (-1 :: Int)]

type MilliSec = Int

setProfilingLevel :: (Conn m) => ProfilingLevel -> Maybe MilliSec -> Db m ()
setProfilingLevel p mSlowMs =
	runCommand (["profile" =: fromEnum p] ++ ("slowms" =? mSlowMs)) >> return ()

-- ** Database

dbStats :: (Conn m) => Db m Document
dbStats = runCommand ["dbstats" =: (1 :: Int)]

currentOp :: (Conn m) => Db m (Maybe Document)
-- ^ See currently running operation on the database, if any
currentOp = findOne (query [] "$cmd.sys.inprog")

type OpNum = Int

killOp :: (Conn m) => OpNum -> Db m (Maybe Document)
killOp op = findOne (query ["op" =: op] "$cmd.sys.killop")

-- ** Server

serverStatus :: (Conn m) => m Document
serverStatus = useDb "admin" $ runCommand ["serverStatus" =: (1 :: Int)]


{- Authors: Tony Hannan <tony@10gen.com>
   Copyright 2010 10gen Inc.
   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. -}
