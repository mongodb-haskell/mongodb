-- | Database administrative functions

{-# LANGUAGE FlexibleContexts, OverloadedStrings, RecordWildCards #-}

module Database.MongoDB.Admin (
    -- * Admin
    -- ** Collection
    CollectionOption(..), createCollection, renameCollection, dropCollection,
    validateCollection,
    -- ** Index
    Index(..), IndexName, index, ensureIndex, createIndex, dropIndex,
    getIndexes, dropIndexes,
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
    ProfilingLevel(..), getProfilingLevel, MilliSec, setProfilingLevel,
    -- ** Database
    dbStats, OpNum, currentOp, killOp,
    -- ** Server
    serverStatus
) where

import Prelude hiding (lookup)
import Control.Applicative ((<$>))
import Control.Concurrent (forkIO, threadDelay)
import Control.Monad (forever, unless, liftM)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Set (Set)
import System.IO.Unsafe (unsafePerformIO)

import qualified Data.HashTable.IO as H
import qualified Data.Set as Set

import Control.Monad.Trans (MonadIO, liftIO)
import Control.Monad.Trans.Control (MonadBaseControl)
import Data.Bson (Document, Field(..), at, (=:), (=?), exclude, merge)
import Data.Text (Text)

import qualified Data.Text as T

import Database.MongoDB.Connection (Host, showHostPort)
import Database.MongoDB.Internal.Protocol (pwHash, pwKey)
import Database.MongoDB.Internal.Util ((<.>), true1)
import Database.MongoDB.Query (Action, Database, Collection, Username, Password,
                               Order, Query(..), accessMode, master, runCommand,
                               useDb, thisDatabase, rest, select, find, findOne,
                               insert_, save, delete)

-- * Admin

-- ** Collection

data CollectionOption = Capped | MaxByteSize Int | MaxItems Int  deriving (Show, Eq)

coptElem :: CollectionOption -> Field
coptElem Capped = "capped" =: True
coptElem (MaxByteSize n) = "size" =: n
coptElem (MaxItems n) = "max" =: n

createCollection :: (MonadIO m) => [CollectionOption] -> Collection -> Action m Document
-- ^ Create collection with given options. You only need to call this to set options, otherwise a collection is created automatically on first use with no options.
createCollection opts col = runCommand $ ["create" =: col] ++ map coptElem opts

renameCollection :: (MonadIO m) => Collection -> Collection -> Action m Document
-- ^ Rename first collection to second collection
renameCollection from to = do
    db <- thisDatabase
    useDb admin $ runCommand ["renameCollection" =: db <.> from, "to" =: db <.> to, "dropTarget" =: True]

dropCollection :: (MonadIO m) => Collection -> Action m Bool
-- ^ Delete the given collection! Return True if collection existed (and was deleted); return False if collection did not exist (and no action).
dropCollection coll = do
    resetIndexCache
    r <- runCommand ["drop" =: coll]
    if true1 "ok" r then return True else do
        if at "errmsg" r == ("ns not found" :: Text) then return False else
            fail $ "dropCollection failed: " ++ show r

validateCollection :: (MonadIO m) => Collection -> Action m Document
-- ^ This operation takes a while
validateCollection coll = runCommand ["validate" =: coll]

-- ** Index

type IndexName = Text

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
genName keys = T.intercalate "_" (map f keys)  where
    f (k := v) = k `T.append` "_" `T.append` T.pack (show v)

ensureIndex :: (MonadIO m) => Index -> Action m ()
-- ^ Create index if we did not already create one. May be called repeatedly with practically no performance hit, because we remember if we already called this for the same index (although this memory gets wiped out every 15 minutes, in case another client drops the index and we want to create it again).
ensureIndex idx = let k = (iColl idx, iName idx) in do
    icache <- fetchIndexCache
    set <- liftIO (readIORef icache)
    unless (Set.member k set) $ do
        accessMode master (createIndex idx)
        liftIO $ writeIORef icache (Set.insert k set)

createIndex :: (MonadIO m) => Index -> Action m ()
-- ^ Create index on the server. This call goes to the server every time.
createIndex idx = insert_ "system.indexes" . idxDocument idx =<< thisDatabase

dropIndex :: (MonadIO m) => Collection -> IndexName -> Action m Document
-- ^ Remove the index
dropIndex coll idxName = do
    resetIndexCache
    runCommand ["deleteIndexes" =: coll, "index" =: idxName]

getIndexes :: (MonadIO m, MonadBaseControl IO m, Functor m) => Collection -> Action m [Document]
-- ^ Get all indexes on this collection
getIndexes coll = do
    db <- thisDatabase
    rest =<< find (select ["ns" =: db <.> coll] "system.indexes")

dropIndexes :: (MonadIO m) => Collection -> Action m Document
-- ^ Drop all indexes on this collection
dropIndexes coll = do
    resetIndexCache
    runCommand ["deleteIndexes" =: coll, "index" =: ("*" :: Text)]

-- *** Index cache

type DbIndexCache = H.BasicHashTable Database IndexCache
-- ^ Cache the indexes we create so repeatedly calling ensureIndex only hits database the first time. Clear cache every once in a while so if someone else deletes index we will recreate it on ensureIndex.

type IndexCache = IORef (Set (Collection, IndexName))

dbIndexCache :: DbIndexCache
-- ^ initialize cache and fork thread that clears it every 15 minutes
dbIndexCache = unsafePerformIO $ do
    table <- H.new
    _ <- forkIO . forever $ threadDelay 900000000 >> clearDbIndexCache
    return table
{-# NOINLINE dbIndexCache #-}

clearDbIndexCache :: IO ()
clearDbIndexCache = do
    keys <- map fst <$> H.toList dbIndexCache
    mapM_ (H.delete dbIndexCache) keys

fetchIndexCache :: (MonadIO m) => Action m IndexCache
-- ^ Get index cache for current database
fetchIndexCache = do
    db <- thisDatabase
    liftIO $ do
        mc <- H.lookup dbIndexCache db
        maybe (newIdxCache db) return mc
 where
    newIdxCache db = do
        idx <- newIORef Set.empty
        H.insert dbIndexCache db idx
        return idx

resetIndexCache :: (MonadIO m) => Action m ()
-- ^ reset index cache for current database
resetIndexCache = do
    icache <- fetchIndexCache
    liftIO (writeIORef icache Set.empty)

-- ** User

allUsers :: (MonadIO m, MonadBaseControl IO m, Functor m) => Action m [Document]
-- ^ Fetch all users of this database
allUsers = map (exclude ["_id"]) <$> (rest =<< find
    (select [] "system.users") {sort = ["user" =: (1 :: Int)], project = ["user" =: (1 :: Int), "readOnly" =: (1 :: Int)]})

addUser :: (MonadIO m) => Bool -> Username -> Password -> Action m ()
-- ^ Add user with password with read-only access if bool is True or read-write access if bool is False
addUser readOnly user pass = do
    mu <- findOne (select ["user" =: user] "system.users")
    let usr = merge ["readOnly" =: readOnly, "pwd" =: pwHash user pass] (maybe ["user" =: user] id mu)
    save "system.users" usr

removeUser :: (MonadIO m) => Username -> Action m ()
removeUser user = delete (select ["user" =: user] "system.users")

-- ** Database

admin :: Database
-- ^ \"admin\" database
admin = "admin"

cloneDatabase :: (MonadIO m) => Database -> Host -> Action m Document
-- ^ Copy database from given host to the server I am connected to. Fails and returns @"ok" = 0@ if we don't have permission to read from given server (use copyDatabase in this case).
cloneDatabase db fromHost = useDb db $ runCommand ["clone" =: showHostPort fromHost]

copyDatabase :: (MonadIO m) => Database -> Host -> Maybe (Username, Password) -> Database -> Action m Document
-- ^ Copy database from given host to the server I am connected to. If username & password is supplied use them to read from given host.
copyDatabase fromDb fromHost mup toDb = do
    let c = ["copydb" =: (1 :: Int), "fromhost" =: showHostPort fromHost, "fromdb" =: fromDb, "todb" =: toDb]
    useDb admin $ case mup of
        Nothing -> runCommand c
        Just (usr, pss) -> do
            n <- at "nonce" `liftM` runCommand ["copydbgetnonce" =: (1 :: Int), "fromhost" =: showHostPort fromHost]
            runCommand $ c ++ ["username" =: usr, "nonce" =: n, "key" =: pwKey n usr pss]

dropDatabase :: (MonadIO m) => Database -> Action m Document
-- ^ Delete the given database!
dropDatabase db = useDb db $ runCommand ["dropDatabase" =: (1 :: Int)]

repairDatabase :: (MonadIO m) => Database -> Action m Document
-- ^ Attempt to fix any corrupt records. This operation takes a while.
repairDatabase db = useDb db $ runCommand ["repairDatabase" =: (1 :: Int)]

-- ** Server

serverBuildInfo :: (MonadIO m) => Action m Document
serverBuildInfo = useDb admin $ runCommand ["buildinfo" =: (1 :: Int)]

serverVersion :: (MonadIO m) => Action m Text
serverVersion = at "version" `liftM` serverBuildInfo

-- * Diagnostics

-- ** Collection

collectionStats :: (MonadIO m) => Collection -> Action m Document
collectionStats coll = runCommand ["collstats" =: coll]

dataSize :: (MonadIO m) => Collection -> Action m Int
dataSize c = at "size" `liftM` collectionStats c

storageSize :: (MonadIO m) => Collection -> Action m Int
storageSize c = at "storageSize" `liftM` collectionStats c

totalIndexSize :: (MonadIO m) => Collection -> Action m Int
totalIndexSize c = at "totalIndexSize" `liftM` collectionStats c

totalSize :: (MonadIO m, MonadBaseControl IO m) => Collection -> Action m Int
totalSize coll = do
    x <- storageSize coll
    xs <- mapM isize =<< getIndexes coll
    return (foldl (+) x xs)
 where
    isize idx = at "storageSize" `liftM` collectionStats (coll `T.append` ".$" `T.append` at "name" idx)

-- ** Profiling

data ProfilingLevel = Off | Slow | All  deriving (Show, Enum, Eq)

getProfilingLevel :: (MonadIO m) => Action m ProfilingLevel
getProfilingLevel = (toEnum . at "was") `liftM` runCommand ["profile" =: (-1 :: Int)]

type MilliSec = Int

setProfilingLevel :: (MonadIO m) => ProfilingLevel -> Maybe MilliSec -> Action m ()
setProfilingLevel p mSlowMs =
    runCommand (["profile" =: fromEnum p] ++ ("slowms" =? mSlowMs)) >> return ()

-- ** Database

dbStats :: (MonadIO m) => Action m Document
dbStats = runCommand ["dbstats" =: (1 :: Int)]

currentOp :: (MonadIO m) => Action m (Maybe Document)
-- ^ See currently running operation on the database, if any
currentOp = findOne (select [] "$cmd.sys.inprog")

type OpNum = Int

killOp :: (MonadIO m) => OpNum -> Action m (Maybe Document)
killOp op = findOne (select ["op" =: op] "$cmd.sys.killop")

-- ** Server

serverStatus :: (MonadIO m) => Action m Document
serverStatus = useDb admin $ runCommand ["serverStatus" =: (1 :: Int)]


{- Authors: Tony Hannan <tony@10gen.com>
   Copyright 2011 10gen Inc.
   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. -}
