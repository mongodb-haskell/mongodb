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

module Database.MongoDB.BSON
    (
     -- * Types
     BsonValue(..),
     BsonDoc,
     BinarySubType(..),
     -- * BsonDoc Operations
     empty,
     -- * Type Conversion
     fromBson, toBson,
     fromBsonDoc, toBsonDoc,
     -- * Binary encoding/decoding
     getBsonDoc, putBsonDoc,
     -- * ObjectId creation
     ObjectIdGen, mkObjectIdGen, genObjectId,
    )
where
import Prelude hiding (lookup)

import qualified Control.Arrow as Arrow
import Control.Exception
import Control.Monad
import Data.Binary
import Data.Binary.Get
import Data.Binary.IEEE754
import Data.Binary.Put
import Data.Bits
import Data.ByteString.Char8 as C8 hiding (empty)
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy.UTF8 as L8
import qualified Data.ByteString.UTF8 as S8
import Data.Digest.OpenSSL.MD5
import Data.Int
import Data.IORef
import qualified Data.List as List
import qualified Data.Map as Map
import Data.Time.Clock.POSIX
import Data.Typeable
import Database.MongoDB.Util
import Network.BSD
import Numeric
import System.IO.Unsafe
import System.Posix.Process

-- | BsonValue is the type that can be used as a value in a 'BsonDoc'.
data BsonValue
    = BsonDouble Double
    | BsonString L8.ByteString
    | BsonDoc BsonDoc
    | BsonArray [BsonValue]
    | BsonUndefined
    | BsonBinary BinarySubType L.ByteString
    | BsonObjectId Integer
    | BsonBool !Bool
    | BsonDate POSIXTime
    | BsonNull
    | BsonRegex L8.ByteString String
    | BsonJSCode L8.ByteString
    | BsonSymbol L8.ByteString
    | BsonJSCodeWScope L8.ByteString BsonDoc
    | BsonInt32 Int32
    | BsonInt64 Int64
    | BsonMinKey
    | BsonMaxKey
    deriving (Show, Eq, Ord)

type BsonDoc = [(L8.ByteString, BsonValue)]

-- | An empty BsonDoc
empty :: BsonDoc
empty = []

data DataType =
    DataMinKey       | -- -1
    DataNumber       | -- 1
    DataString       | -- 2
    DataDoc          | -- 3
    DataArray        | -- 4
    DataBinary       | -- 5
    DataUndefined    | -- 6
    DataOid          | -- 7
    DataBoolean      | -- 8
    DataDate         | -- 9
    DataNull         | -- 10
    DataRegex        | -- 11
    DataRef          | -- 12
    DataJSCode       | -- 13
    DataSymbol       | -- 14
    DataJSCodeWScope | -- 15
    DataInt          | -- 16
    DataTimestamp    | -- 17
    DataLong         | -- 18
    DataMaxKey         -- 127
    deriving (Show, Read, Enum, Eq, Ord)

toDataType :: Int -> DataType
toDataType (-1) = DataMinKey
toDataType 127 = DataMaxKey
toDataType d = toEnum d

fromDataType :: DataType -> Int8
fromDataType DataMinKey = - 1
fromDataType DataMaxKey = 127
fromDataType d = fromIntegral $ fromEnum d

data BinarySubType =
    BSTUNDEFINED1      |
    BSTFunction        | -- 1
    BSTByteArray       | -- 2
    BSTUUID            | -- 3
    BSTUNDEFINED2      |
    BSTMD5             | -- 5
    BSTUserDefined
    deriving (Show, Read, Enum, Eq, Ord)

toBinarySubType :: Int -> BinarySubType
toBinarySubType 0x80 = BSTUserDefined
toBinarySubType d = toEnum d

fromBinarySubType :: BinarySubType -> Int8
fromBinarySubType BSTUserDefined = 0x80
fromBinarySubType d = fromIntegral $ fromEnum d

data ObjectIdGen = ObjectIdGen {
      oigMachine :: Integer,
      oigInc :: IORef Integer
    }

globalObjectIdInc :: IORef Integer
{-# NOINLINE globalObjectIdInc #-}
globalObjectIdInc = unsafePerformIO (newIORef 0)

-- | Create a new 'ObjectIdGen', the structure that must be passed to
-- genObjectId to create a 'ObjectId'.
mkObjectIdGen :: IO ObjectIdGen
mkObjectIdGen = do
  host <- liftM (fst . (!! 0) . readHex . List.take 6 . md5sum . C8.pack)
                getHostName
  return ObjectIdGen {oigMachine = host, oigInc = globalObjectIdInc}

-- | Create a new 'ObjectId'.
genObjectId :: ObjectIdGen -> IO BsonValue
genObjectId oig = do
  now <- liftM (truncate . (realToFrac :: POSIXTime -> Double)) getPOSIXTime
  pid <- liftM fromIntegral getProcessID
  inc <- atomicModifyIORef (oigInc oig) $ \i -> ((i+1) `rem` 0x10000, i)
  return $ BsonObjectId
             ((now `shiftL` 64) .|.
              oigMachine oig `shiftL` 40 .|.
              (0xffff .&. pid) `shiftL` 24 .|.
              (inc `div` 0x100) `shiftL` 8 .|. (inc `rem` 0x100))

-- | Decode binary bytes into 'BsonDoc'.
getBsonDoc :: Get BsonDoc
getBsonDoc = liftM snd getDoc

-- | Encode 'BsonDoc' into binary bytes.
putBsonDoc :: BsonDoc -> Put
putBsonDoc = putObj

getVal :: DataType -> Get (Integer, BsonValue)
getVal DataNumber = liftM ((,) 8 . BsonDouble) getFloat64le
getVal DataString = do
  sLen1 <- getI32
  (_sLen2, s) <- getS
  return (fromIntegral $ 4 + sLen1, BsonString s)
getVal DataDoc = getDoc >>= \(len, obj) -> return (len, BsonDoc obj)
getVal DataArray = do
  bytes <- getI32
  arr <- getInnerArray (bytes - 4)
  getNull
  return (fromIntegral bytes, BsonArray arr)
getVal DataBinary = do
  len1 <- getI32
  st <- getI8
  (len, hdrLen) <- if toBinarySubType st == BSTByteArray
                     then do
                       len2 <- getI32
                       assert (len1 - 4 == len2) $ return ()
                       return (len2, 4 + 1 + 4)
                     else return (len1, 4 + 1)
  bs <- getLazyByteString $ fromIntegral len
  return (hdrLen + fromIntegral len, BsonBinary (toBinarySubType st) bs)

getVal DataUndefined = return (1, BsonUndefined)
getVal DataOid = do
  oid1 <- getWord64be
  oid2 <- getWord32be
  let oid = (fromIntegral oid1 `shiftL` 32) .|. fromIntegral oid2
  return (12, BsonObjectId oid)
getVal DataBoolean = liftM ((,) (1::Integer) . BsonBool . (/= (0::Int))) getI8
getVal DataDate = liftM ((,) 8 . BsonDate . flip (/) 1000 . realToFrac) getI64
getVal DataNull = return (1, BsonNull)
getVal DataRegex = fail "DataJSCode not yet supported" -- TODO
getVal DataRef = fail "DataRef is deprecated"
getVal DataJSCode = do
  sLen1 <- getI32
  (_sLen2, s) <- getS
  return (fromIntegral $ 4 + sLen1, BsonJSCode s)
getVal DataSymbol = do
  sLen1 <- getI32
  (_sLen2, s) <- getS
  return (fromIntegral $ 4 + sLen1, BsonString s)
getVal DataJSCodeWScope = do
  sLen1 <- getI32
  (_, qry) <- getS
  (_, scope) <- getDoc
  return (fromIntegral sLen1, BsonJSCodeWScope qry scope)
getVal DataInt = liftM ((,) 4 . BsonInt32 . fromIntegral) getI32
getVal DataTimestamp = fail "DataTimestamp not yet supported" -- TODO

getVal DataLong = liftM ((,) 8 . BsonInt64) getI64
getVal DataMinKey = return (0, BsonMinKey)
getVal DataMaxKey = return (0, BsonMaxKey)

getInnerObj :: Int32 -> Get BsonDoc
getInnerObj 1 = return []
getInnerObj bytesLeft = do
  typ <- getDataType
  (keySz, key) <- getS
  (valSz, val) <- getVal typ
  rest <- getInnerObj (bytesLeft - 1 - fromIntegral keySz - fromIntegral valSz)
  return $ (key, val) : rest

getRawObj :: Get (Integer, BsonDoc)
getRawObj = do
  bytes <- getI32
  obj <- getInnerObj (bytes - 4)
  getNull
  return (fromIntegral bytes, obj)

getInnerArray :: Int32 -> Get [BsonValue]
getInnerArray 1 = return []
getInnerArray bytesLeft = do
  typ <- getDataType
  (keySz, _key) <- getS
  (valSz, val) <- getVal typ
  rest <- getInnerArray
          (bytesLeft - 1 - fromIntegral keySz - fromIntegral valSz)
  return $ val : rest

getDoc :: Get (Integer, BsonDoc)
getDoc = getRawObj

getDataType :: Get DataType
getDataType = liftM toDataType getI8

putType :: BsonValue -> Put
putType BsonDouble{}   = putDataType DataNumber
putType BsonString{}   = putDataType DataString
putType BsonDoc{}      = putDataType DataDoc
putType BsonArray{}    = putDataType DataArray
putType BsonBinary{}   = putDataType DataBinary
putType BsonUndefined  = putDataType DataUndefined
putType BsonObjectId{} = putDataType DataOid
putType BsonBool{}     = putDataType DataBoolean
putType BsonDate{}     = putDataType DataDate
putType BsonNull       = putDataType DataNull
putType BsonRegex{}    = putDataType DataRegex
-- putType = putDataType DataRef
putType BsonJSCode {}  = putDataType DataJSCode
putType BsonSymbol{}   = putDataType DataSymbol
putType BsonJSCodeWScope{} = putDataType DataJSCodeWScope
putType BsonInt32 {}   = putDataType DataInt
putType BsonInt64 {}   = putDataType DataLong
-- putType = putDataType DataTimestamp
putType BsonMinKey     = putDataType DataMinKey
putType BsonMaxKey     = putDataType DataMaxKey

putVal :: BsonValue -> Put
putVal (BsonDouble d)   = putFloat64le d
putVal (BsonString s)   = putStrSz s
putVal (BsonDoc o)      = putObj o
putVal (BsonArray es)   = putOutterObj bs
    where bs = runPut $ forM_ (List.zip [(0::Int) .. ] es) $ \(i, e) ->
               putType e >> putS (L8.fromString $ show i) >> putVal e
putVal (BsonBinary t bs) = do
  putI32 $ fromIntegral $ (if t == BSTByteArray then 4 else 0) + L.length bs
  putI8 $ fromBinarySubType t
  when (t == BSTByteArray) $ putI32 $ fromIntegral $ L.length bs
  putLazyByteString bs
putVal BsonUndefined    = putNothing
putVal (BsonObjectId o) = putWord64be (fromIntegral $ o `shiftR` 32) >>
                          putWord32be (fromIntegral $ o .&. 0xffffffff)
putVal (BsonBool False) = putI8 0
putVal (BsonBool True)  = putI8 1
putVal (BsonDate pt)    = putI64 $ round $ 1000 * (realToFrac pt :: Double)
putVal BsonNull         = putNothing
putVal (BsonRegex r opt)= do putS r
                             putByteString $ pack $ List.sort opt
                             putNull
putVal (BsonJSCode c)   = putStrSz c
putVal (BsonSymbol s)   = putStrSz s
putVal (BsonJSCodeWScope q s) =
  let bytes = runPut (putStrSz q >> putObj s)
    in putI32 ((+4) $ fromIntegral $ L.length bytes) >> putLazyByteString bytes
putVal (BsonInt32 i)    = putI32 i
putVal (BsonInt64 i)    = putI64 i
putVal BsonMinKey       = putNothing
putVal BsonMaxKey       = putNothing

putObj :: BsonDoc -> Put
putObj obj   = putOutterObj bs
    where bs = runPut $ forM_ obj $ \(k, v) -> putType v >> putS k >> putVal v

putOutterObj :: L.ByteString -> Put
putOutterObj bytes = do
  -- the length prefix and null term are included in the length
  putI32 $ fromIntegral $ 4 + 1 + L.length bytes
  putLazyByteString bytes
  putNull

putDataType :: DataType -> Put
putDataType = putI8 . fromDataType

class BsonDocConv a where
    -- | Convert a BsonDoc into another form such as a Map or a tuple
    -- list with String keys.
    fromBsonDoc :: BsonDoc -> a
    -- | Convert a Map or a tuple list with String keys into a BsonDoc.
    toBsonDoc :: a -> BsonDoc

instance BsonDocConv [(L8.ByteString, BsonValue)] where
    fromBsonDoc = id
    toBsonDoc = id

instance BsonDocConv [(String, BsonValue)] where
    fromBsonDoc = List.map $ Arrow.first L8.toString
    toBsonDoc = List.map $ Arrow.first L8.fromString

instance BsonDocConv (Map.Map L8.ByteString BsonValue) where
    fromBsonDoc = Map.fromList
    toBsonDoc = Map.toList

instance BsonDocConv (Map.Map String BsonValue) where
    fromBsonDoc = Map.fromList . fromBsonDoc
    toBsonDoc = toBsonDoc . Map.toList

data BsonUnsupportedConversion = BsonUnsupportedConversion
                                 deriving (Eq, Show, Read)

bsonUnsupportedConversion :: TyCon
bsonUnsupportedConversion =
    mkTyCon "Database.MongoDB.BSON.BsonUnsupportedConversion "

instance Typeable BsonUnsupportedConversion where
    typeOf _ = mkTyConApp bsonUnsupportedConversion []

instance Exception BsonUnsupportedConversion

throwUnsupConv :: a
throwUnsupConv = throw BsonUnsupportedConversion

class BsonConv a where
    -- | Convert a native Haskell type into a BsonValue.
    toBson :: a -> BsonValue
    -- | Convert a BsonValue into a native Haskell type.
    fromBson :: BsonValue -> a

instance BsonConv Double where
    toBson = BsonDouble
    fromBson (BsonDouble d) = d
    fromBson _ = throwUnsupConv

instance BsonConv Float where
    toBson = BsonDouble . realToFrac
    fromBson (BsonDouble d) = realToFrac d
    fromBson _ = throwUnsupConv

instance BsonConv L8.ByteString where
    toBson = BsonString
    fromBson (BsonString s) = s
    fromBson _ = throwUnsupConv

instance BsonConv String where
    toBson = BsonString . L8.fromString
    fromBson (BsonString s) = L8.toString s
    fromBson _ = throwUnsupConv

instance BsonConv S8.ByteString where
    toBson bs = BsonString $ L.fromChunks [bs]
    fromBson (BsonString s) = C8.concat $ L.toChunks s
    fromBson _ = throwUnsupConv

instance  BsonConv BsonDoc where
    toBson = BsonDoc
    fromBson (BsonDoc d) = d
    fromBson _ = throwUnsupConv

instance BsonConv [(String, BsonValue)] where
    toBson = toBson . toBsonDoc
    fromBson (BsonDoc d) = fromBsonDoc d
    fromBson _ = throwUnsupConv

instance BsonConv (Map.Map L8.ByteString BsonValue) where
    toBson = toBson . toBsonDoc
    fromBson (BsonDoc d) = fromBsonDoc d
    fromBson _ = throwUnsupConv

instance BsonConv (Map.Map String BsonValue) where
    toBson = toBson . toBsonDoc
    fromBson (BsonDoc d) = fromBsonDoc d
    fromBson _ = throwUnsupConv

instance BsonConv POSIXTime where
    toBson = BsonDate
    fromBson (BsonDate d) = d
    fromBson _ = throwUnsupConv

instance BsonConv Bool where
    toBson = BsonBool
    fromBson (BsonBool b) = b
    fromBson _ = throwUnsupConv

instance BsonConv Int where
    toBson i | i >= fromIntegral (minBound::Int32) &&
               i <= fromIntegral (maxBound::Int32) = BsonInt32 $ fromIntegral i
             | otherwise = BsonInt64 $ fromIntegral i
    fromBson (BsonInt32 i) = fromIntegral i
    fromBson (BsonInt64 i) = fromIntegral i
    fromBson _ = throwUnsupConv

instance BsonConv Int8 where
    toBson i | i >= fromIntegral (minBound::Int32) &&
               i <= fromIntegral (maxBound::Int32) = BsonInt32 $ fromIntegral i
             | otherwise = BsonInt64 $ fromIntegral i
    fromBson (BsonInt32 i) = fromIntegral i
    fromBson (BsonInt64 i) = fromIntegral i
    fromBson _ = throwUnsupConv

instance BsonConv Int16 where
    toBson i | i >= fromIntegral (minBound::Int32) &&
               i <= fromIntegral (maxBound::Int32) = BsonInt32 $ fromIntegral i
             | otherwise = BsonInt64 $ fromIntegral i
    fromBson (BsonInt32 i) = fromIntegral i
    fromBson (BsonInt64 i) = fromIntegral i
    fromBson _ = throwUnsupConv

instance BsonConv Int32 where
    toBson i | i >= fromIntegral (minBound::Int32) &&
               i <= fromIntegral (maxBound::Int32) = BsonInt32 $ fromIntegral i
             | otherwise = BsonInt64 $ fromIntegral i
    fromBson (BsonInt32 i) = fromIntegral i
    fromBson (BsonInt64 i) = fromIntegral i
    fromBson _ = throwUnsupConv

instance BsonConv Int64 where
    toBson i | i >= fromIntegral (minBound::Int32) &&
               i <= fromIntegral (maxBound::Int32) = BsonInt32 $ fromIntegral i
             | otherwise = BsonInt64 $ fromIntegral i
    fromBson (BsonInt32 i) = fromIntegral i
    fromBson (BsonInt64 i) = fromIntegral i
    fromBson _ = throwUnsupConv

instance BsonConv Integer where
    toBson i | i >= fromIntegral (minBound::Int32) &&
               i <= fromIntegral (maxBound::Int32) = BsonInt32 $ fromIntegral i
             | otherwise = BsonInt64 $ fromIntegral i
    fromBson (BsonInt32 i) = fromIntegral i
    fromBson (BsonInt64 i) = fromIntegral i
    fromBson _ = throwUnsupConv

instance BsonConv Word where
    toBson i | i >= fromIntegral (minBound::Int32) &&
               i <= fromIntegral (maxBound::Int32) = BsonInt32 $ fromIntegral i
             | otherwise = BsonInt64 $ fromIntegral i
    fromBson (BsonInt32 i) = fromIntegral i
    fromBson (BsonInt64 i) = fromIntegral i
    fromBson _ = throwUnsupConv

instance BsonConv Word8 where
    toBson i | i >= fromIntegral (minBound::Int32) &&
               i <= fromIntegral (maxBound::Int32) = BsonInt32 $ fromIntegral i
             | otherwise = BsonInt64 $ fromIntegral i
    fromBson (BsonInt32 i) = fromIntegral i
    fromBson (BsonInt64 i) = fromIntegral i
    fromBson _ = throwUnsupConv

instance BsonConv Word16 where
    toBson i | i >= fromIntegral (minBound::Int32) &&
               i <= fromIntegral (maxBound::Int32) = BsonInt32 $ fromIntegral i
             | otherwise = BsonInt64 $ fromIntegral i
    fromBson (BsonInt32 i) = fromIntegral i
    fromBson (BsonInt64 i) = fromIntegral i
    fromBson _ = throwUnsupConv

instance BsonConv Word32 where
    toBson i | i >= fromIntegral (minBound::Int32) &&
               i <= fromIntegral (maxBound::Int32) = BsonInt32 $ fromIntegral i
             | otherwise = BsonInt64 $ fromIntegral i
    fromBson (BsonInt32 i) = fromIntegral i
    fromBson (BsonInt64 i) = fromIntegral i
    fromBson _ = throwUnsupConv

instance BsonConv Word64 where
    toBson i | i >= fromIntegral (minBound::Int32) &&
               i <= fromIntegral (maxBound::Int32) = BsonInt32 $ fromIntegral i
             | otherwise = BsonInt64 $ fromIntegral i
    fromBson (BsonInt32 i) = fromIntegral i
    fromBson (BsonInt64 i) = fromIntegral i
    fromBson _ = throwUnsupConv

instance BsonConv [Double] where
    toBson = BsonArray . List.map toBson
    fromBson (BsonArray ss) = List.map fromBson ss
    fromBson _ = throwUnsupConv

instance BsonConv [Float] where
    toBson = BsonArray . List.map toBson
    fromBson (BsonArray ss) = List.map fromBson ss
    fromBson _ = throwUnsupConv

instance BsonConv [Int] where
    toBson = BsonArray . List.map toBson
    fromBson (BsonArray ss) = List.map fromBson ss
    fromBson _ = throwUnsupConv

instance BsonConv [Int8] where
    toBson = BsonArray . List.map toBson
    fromBson (BsonArray ss) = List.map fromBson ss
    fromBson _ = throwUnsupConv

instance BsonConv [Int16] where
    toBson = BsonArray . List.map toBson
    fromBson (BsonArray ss) = List.map fromBson ss
    fromBson _ = throwUnsupConv

instance BsonConv [Int32] where
    toBson = BsonArray . List.map toBson
    fromBson (BsonArray ss) = List.map fromBson ss
    fromBson _ = throwUnsupConv

instance BsonConv [Int64] where
    toBson = BsonArray . List.map toBson
    fromBson (BsonArray ss) = List.map fromBson ss
    fromBson _ = throwUnsupConv

instance BsonConv [Integer] where
    toBson = BsonArray . List.map toBson
    fromBson (BsonArray ss) = List.map fromBson ss
    fromBson _ = throwUnsupConv

instance BsonConv [Word] where
    toBson = BsonArray . List.map toBson
    fromBson (BsonArray ss) = List.map fromBson ss
    fromBson _ = throwUnsupConv

instance BsonConv [Word8] where
    toBson = BsonArray . List.map toBson
    fromBson (BsonArray ss) = List.map fromBson ss
    fromBson _ = throwUnsupConv

instance BsonConv [Word16] where
    toBson = BsonArray . List.map toBson
    fromBson (BsonArray ss) = List.map fromBson ss
    fromBson _ = throwUnsupConv

instance BsonConv [Word32] where
    toBson = BsonArray . List.map toBson
    fromBson (BsonArray ss) = List.map fromBson ss
    fromBson _ = throwUnsupConv

instance BsonConv [Word64] where
    toBson = BsonArray . List.map toBson
    fromBson (BsonArray ss) = List.map fromBson ss
    fromBson _ = throwUnsupConv

instance BsonConv [Bool] where
    toBson = BsonArray . List.map toBson
    fromBson (BsonArray ss) = List.map fromBson ss
    fromBson _ = throwUnsupConv

instance BsonConv [POSIXTime] where
    toBson = BsonArray . List.map toBson
    fromBson (BsonArray ss) = List.map fromBson ss
    fromBson _ = throwUnsupConv

instance BsonConv [String] where
    toBson = BsonArray . List.map toBson
    fromBson (BsonArray ss) = List.map fromBson ss
    fromBson _ = throwUnsupConv

instance BsonConv [L8.ByteString] where
    toBson = BsonArray . List.map toBson
    fromBson (BsonArray ss) = List.map fromBson ss
    fromBson _ = throwUnsupConv

instance BsonConv [S8.ByteString] where
    toBson = BsonArray . List.map toBson
    fromBson (BsonArray ss) = List.map fromBson ss
    fromBson _ = throwUnsupConv

instance BsonConv [BsonDoc] where
    toBson = BsonArray . List.map toBson
    fromBson (BsonArray ss) = List.map fromBson ss
    fromBson _ = throwUnsupConv

instance (BsonConv a) => BsonConv (Maybe a) where
    toBson Nothing = BsonNull
    toBson (Just a) = toBson a
    fromBson BsonNull = Nothing
    fromBson a = Just $ fromBson a
