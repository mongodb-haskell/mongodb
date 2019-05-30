-- Author:
-- Brent Tubbs <brent.tubbs@gmail.com>
-- | MongoDB GridFS implementation
{-# LANGUAGE OverloadedStrings, RecordWildCards, NamedFieldPuns, TupleSections, FlexibleContexts, FlexibleInstances, UndecidableInstances, MultiParamTypeClasses, GeneralizedNewtypeDeriving, StandaloneDeriving, TypeSynonymInstances, TypeFamilies, CPP, RankNTypes #-}

module Database.MongoDB.GridFS
  ( Bucket
  , files, chunks
  , File
  , document, bucket
  -- ** Setup
  , openDefaultBucket
  , openBucket
  -- ** Query
  , findFile
  , findOneFile
  , fetchFile
  -- ** Delete
  , deleteFile
  -- ** Conduits
  , sourceFile
  , sinkFile
  )
  where

import Control.Applicative((<$>))

import Control.Monad(when)
import Control.Monad.IO.Class
import Control.Monad.Trans(lift)

import Data.Conduit
import Data.Digest.Pure.MD5
import Data.Int
import Data.Tagged(Tagged, untag)
import Data.Text(Text, append)
import Data.Time.Clock(getCurrentTime)
import Database.MongoDB
import Prelude
import qualified Data.Bson as B
import qualified Data.ByteString as S
import qualified Data.ByteString.Lazy as L


defaultChunkSize :: Int64
-- ^ The default chunk size is 256 kB
defaultChunkSize = 256 * 1024

-- magic constant for md5Finalize
md5BlockSizeInBytes :: Int
md5BlockSizeInBytes = 64


data Bucket = Bucket {files :: Text, chunks :: Text}
-- ^ Files are stored in "buckets". You open a bucket with openDefaultBucket or openBucket

openDefaultBucket :: (Monad m, MonadIO m) => Action m Bucket
-- ^ Open the default 'Bucket' (named "fs")
openDefaultBucket = openBucket "fs"

openBucket :: (Monad m, MonadIO m) => Text -> Action m Bucket
-- ^ Open a 'Bucket'
openBucket name = do
  let filesCollection = name `append` ".files"
  let chunksCollection = name `append` ".chunks"
  ensureIndex $ (index filesCollection ["filename" =: (1::Int), "uploadDate" =: (1::Int)])
  ensureIndex $ (index chunksCollection ["files_id" =: (1::Int), "n" =: (1::Int)]) { iUnique = True, iDropDups = True }
  return $ Bucket filesCollection chunksCollection

data File = File {bucket :: Bucket, document :: Document}

getChunk :: (Monad m, MonadIO m) => File -> Int -> Action m (Maybe S.ByteString)
-- ^ Get a chunk of a file
getChunk (File bucket doc) i = do
  files_id <- B.look "_id" doc
  result <- findOne $ select ["files_id" := files_id, "n" =: i] $ chunks bucket
  let content = at "data" <$> result
  case content of
    Just (Binary b) -> return (Just b)
    _ -> return Nothing

findFile :: MonadIO m => Bucket -> Selector -> Action m [File]
-- ^ Find files in the bucket
findFile bucket sel = do
  cursor <- find $ select sel $ files bucket
  results <- rest cursor
  return $ File bucket <$> results

findOneFile :: MonadIO m => Bucket -> Selector -> Action m (Maybe File)
-- ^ Find one file in the bucket
findOneFile bucket sel = do
  mdoc <- findOne $ select sel $ files bucket
  return $ File bucket <$> mdoc

fetchFile :: MonadIO m => Bucket -> Selector -> Action m File
-- ^ Fetch one file in the bucket
fetchFile bucket sel = do
  doc <- fetch $ select sel $ files bucket
  return $ File bucket doc

deleteFile :: (MonadIO m) => File -> Action m ()
-- ^ Delete files in the bucket
deleteFile (File bucket doc) = do
  files_id <- B.look "_id" doc
  delete $ select ["_id" := files_id] $ files bucket
  delete $ select ["files_id" := files_id] $ chunks bucket

putChunk :: (Monad m, MonadIO m) => Bucket -> ObjectId -> Int -> L.ByteString -> Action m ()
-- ^ Put a chunk in the bucket
putChunk bucket files_id i chunk = do
  insert_ (chunks bucket) ["files_id" =: files_id, "n" =: i, "data" =: Binary (L.toStrict chunk)]

sourceFile :: (Monad m, MonadIO m) => File -> ConduitT i S.ByteString (Action m) ()
-- ^ A producer for the contents of a file
sourceFile file = yieldChunk 0 where
  yieldChunk i = do
    mbytes <- lift $ getChunk file i
    case mbytes of
      Just bytes -> yield bytes >> yieldChunk (i+1)
      Nothing -> return ()

-- Used to keep data during writing
data FileWriter = FileWriter
  { _fwChunkSize :: Int64
  , _fwBucket :: Bucket
  , _fwFilesId :: ObjectId
  , _fwChunkIndex :: Int
  , _fwSize :: Int64
  , _fwAcc :: L.ByteString
  , _fwMd5Context :: MD5Context
  , _fwMd5acc :: L.ByteString
  }

-- Finalize file, calculating md5 digest, saving the last chunk, and creating the file in the bucket
finalizeFile :: (Monad m, MonadIO m) => Text -> FileWriter -> Action m File
finalizeFile filename (FileWriter chunkSize bucket files_id i size acc md5context md5acc) = do
  let md5digest = finalizeMD5 md5context (L.toStrict md5acc)
  when (L.length acc > 0) $ putChunk bucket files_id i acc
  timestamp <- liftIO $ getCurrentTime
  let doc = [ "_id" =: files_id
            , "length" =: size
            , "uploadDate" =: timestamp
            , "md5" =: show (md5digest)
            , "chunkSize" =: chunkSize
            , "filename" =: filename
            ]
  insert_ (files bucket) doc
  return $ File bucket doc

-- finalize the remainder and return the MD5Digest.
finalizeMD5 :: MD5Context -> S.ByteString -> MD5Digest
finalizeMD5 ctx rest =
  md5Finalize ctx2 (S.drop lu rest) -- can only handle max md5BlockSizeInBytes length
  where
    l = S.length rest
    r = l `mod` md5BlockSizeInBytes
    lu = l - r
    ctx2 = md5Update ctx (S.take lu rest)

-- Write as many chunks as can be written from the file writer
writeChunks :: (Monad m, MonadIO m) => FileWriter -> L.ByteString -> Action m FileWriter
writeChunks (FileWriter chunkSize bucket files_id i size acc md5context md5acc) chunk = do
  -- Update md5 context
  let md5BlockLength = fromIntegral $ untag (blockLength :: Tagged MD5Digest Int)
  let md5acc_temp = (md5acc `L.append` chunk)
  let (md5context', md5acc') = 
        if (L.length md5acc_temp < md5BlockLength)
        then (md5context, md5acc_temp)
        else let numBlocks = L.length md5acc_temp `div` md5BlockLength
                 (current, rest) = L.splitAt (md5BlockLength * numBlocks) md5acc_temp
             in (md5Update md5context (L.toStrict current), rest)
  -- Update chunks
  let size' = (size + L.length chunk)
  let acc_temp = (acc `L.append` chunk)
  if (L.length acc_temp < chunkSize)
    then return (FileWriter chunkSize bucket files_id i size' acc_temp md5context' md5acc')
    else do
      let (chunk, acc') = L.splitAt chunkSize acc_temp
      putChunk bucket files_id i chunk
      writeChunks (FileWriter chunkSize bucket files_id (i+1) size' acc' md5context' md5acc') L.empty

sinkFile :: (Monad m, MonadIO m) => Bucket -> Text -> ConduitT S.ByteString o (Action m) File
-- ^ A consumer that creates a file in the bucket and puts all consumed data in it
sinkFile bucket filename = do
  files_id <- liftIO $ genObjectId
  awaitChunk $ FileWriter defaultChunkSize bucket files_id 0 0 L.empty md5InitialContext L.empty
 where
  awaitChunk fw = do
    mchunk <- await
    case mchunk of
      Nothing -> lift (finalizeFile filename fw)
      Just chunk -> lift (writeChunks fw (L.fromStrict chunk)) >>= awaitChunk
