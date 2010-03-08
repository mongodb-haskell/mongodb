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
     empty, lookup,
     -- * Type Conversion
     fromBson, toBson,
     fromBsonDoc, toBsonDoc,
     -- * Binary encoding/decoding
     getBsonDoc, putBsonDoc,
    )
where
import Prelude hiding (lookup)

import Control.Monad
import Data.Binary
import Data.Binary.Get
import Data.Binary.IEEE754
import Data.Binary.Put
import Data.ByteString.Char8 as C8 hiding (empty)
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy.UTF8 as L8
import qualified Data.ByteString.UTF8 as S8
import Data.Convertible
import Data.Int
import qualified Data.Map as Map
import qualified Data.List as List
import Data.Time.Clock.POSIX
import Data.Typeable
import Database.MongoDB.Util

-- | BsonValue is the type that can be used as a key in a 'BsonDoc'.
data BsonValue
    = BsonDouble Double
    | BsonString L8.ByteString
    | BsonDoc BsonDoc
    | BsonArray [BsonValue]
    | BsonUndefined
    | BsonBinary BinarySubType L.ByteString
    | BsonObjectId L.ByteString
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

instance Typeable BsonValue where
    typeOf _ = mkTypeName "BsonValue"

-- | BSON Document: this is the top-level (but recursive) type that
-- all MongoDB collections work in terms of. It is a mapping between
-- strings ('Data.ByteString.Lazu.UTF8.ByteString') and 'BsonValue's.
-- It can be constructed either from a 'Map' (eg @'BsonDoc' myMap@) or
-- from a associative list (eg @'toBsonDoc' myAL@).
type BsonDoc = Map.Map L8.ByteString BsonValue

class BsonDocOps a where
    -- | Construct a BsonDoc from an associative list
    toBsonDoc :: [(a, BsonValue)] -> BsonDoc
    -- | Unwrap BsonDoc to be a Map
    fromBsonDoc :: BsonDoc -> [(a, BsonValue)]
    -- | Return the BsonValue for given key, if any.
    lookup :: a -> BsonDoc -> Maybe BsonValue

-- | An empty BsonDoc
empty :: BsonDoc
empty = Map.empty

instance BsonDocOps L8.ByteString where
    toBsonDoc = Map.fromList
    fromBsonDoc = Map.toList
    lookup = Map.lookup

instance BsonDocOps String where
    toBsonDoc = Map.mapKeys L8.fromString .Map.fromList
    fromBsonDoc = Map.toList . Map.mapKeys L8.toString
    lookup = Map.lookup . L8.fromString

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

fromDataType :: DataType -> Int
fromDataType DataMinKey = - 1
fromDataType DataMaxKey = 127
fromDataType d = fromEnum d

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

fromBinarySubType :: BinarySubType -> Int
fromBinarySubType BSTUserDefined = 0x80
fromBinarySubType d = fromEnum d

getBsonDoc :: Get BsonDoc
getBsonDoc = liftM snd getDoc

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
  (len, arr) <- getRawObj
  let arr2 = Map.fold (:) [] arr -- reverse and remove key
  return (len, BsonArray arr2)
getVal DataBinary = do
  skip 4
  st   <- getI8
  len2 <- getI32
  bs   <- getLazyByteString $ fromIntegral len2
  return (4 + 1 + 4 + fromIntegral len2, BsonBinary (toBinarySubType st) bs)
getVal DataUndefined = return (1, BsonUndefined)
getVal DataOid = liftM ((,) 12 . BsonObjectId) $ getLazyByteString 12
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

getInnerObj :: Int32 -> BsonDoc -> Get BsonDoc
getInnerObj 1 obj = return obj
getInnerObj bytesLeft obj = do
  typ <- getDataType
  (keySz, key) <- getS
  (valSz, val) <- getVal typ
  getInnerObj (bytesLeft - 1 - fromIntegral keySz - fromIntegral valSz) $
              Map.insert key val obj

getRawObj :: Get (Integer, BsonDoc)
getRawObj = do
  bytes <- getI32
  obj <- getInnerObj (bytes - 4) empty
  getNull
  return (fromIntegral bytes, obj)

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
putVal (BsonBinary t bs)= do putI32 $ fromIntegral $ 4 + L.length bs
                             putI8 $ fromBinarySubType t
                             putI32 $ fromIntegral $ L.length bs
                             putLazyByteString bs
putVal BsonUndefined    = putNothing
putVal (BsonObjectId o) = putLazyByteString o
putVal (BsonBool False) = putI8 (0::Int)
putVal (BsonBool True)  = putI8 (1::Int)
putVal (BsonDate pt)    = putI64 $ round $ 1000 * (realToFrac pt :: Double)
putVal BsonNull         = putNothing
putVal (BsonRegex r opt)= do putS r
                             putByteString $ pack $ List.sort opt
                             putNull
putVal (BsonJSCode c)   = putStrSz c
putVal (BsonSymbol s)   = putI32 (fromIntegral $ 1 + L8.length s) >> putS s
putVal (BsonJSCodeWScope q s) =
  let bytes = runPut (putStrSz q >> putObj s)
    in putI32 ((+4) $ fromIntegral $ L.length bytes) >> putLazyByteString bytes
putVal (BsonInt32 i)    = putI32 i
putVal (BsonInt64 i)    = putI64 i
putVal BsonMinKey       = putNothing
putVal BsonMaxKey       = putNothing

putObj :: BsonDoc -> Put
putObj obj   = putOutterObj bs
    where bs = runPut $ forM_ (fromBsonDoc obj) $ \(k, v) ->
               putType v >> putS k >> putVal v

putOutterObj :: L.ByteString -> Put
putOutterObj bytes = do
  -- the length prefix and null term are included in the length
  putI32 $ fromIntegral $ 4 + 1 + L.length bytes
  putLazyByteString bytes
  putNull

putDataType :: DataType -> Put
putDataType = putI8 . fromDataType

class BsonConv a b where
    -- | Convert a BsonValue into a native Haskell type.
    fromBson :: Convertible a b => a -> b
    -- | Convert a native Haskell type into a BsonValue.
    toBson :: Convertible b a => b -> a

instance BsonConv BsonValue a where
    fromBson = convert
    toBson = convert

instance BsonConv (Maybe BsonValue) (Maybe a) where
    fromBson = convert
    toBson = convert

unsupportedError :: (Typeable a, Convertible BsonValue a) =>
                    BsonValue -> ConvertResult a
unsupportedError = convError "Unsupported conversion"

instance Convertible Double BsonValue where
    safeConvert = return . BsonDouble

instance Convertible Float BsonValue where
    safeConvert = return . BsonDouble . realToFrac

instance Convertible String BsonValue where
    safeConvert = return . BsonString . L8.fromString

instance Convertible L8.ByteString BsonValue where
    safeConvert = return . BsonString

instance Convertible S8.ByteString BsonValue where
    safeConvert = return . BsonString . L.fromChunks . return

instance Convertible [Double] BsonValue where
    safeConvert ds = BsonArray `liftM` mapM safeConvert ds

instance Convertible [Float] BsonValue where
    safeConvert fs = BsonArray `liftM` mapM safeConvert fs

instance Convertible [String] BsonValue where
    safeConvert ss = BsonArray `liftM` mapM safeConvert ss

instance Convertible [L8.ByteString] BsonValue where
    safeConvert bs = BsonArray `liftM` mapM safeConvert bs

instance Convertible [S8.ByteString] BsonValue where
    safeConvert bs = BsonArray `liftM` mapM safeConvert bs

instance Convertible BsonDoc BsonValue where
    safeConvert = return . BsonDoc

instance Convertible (Map.Map String BsonValue) BsonValue where
    safeConvert = return . BsonDoc . Map.mapKeys L8.fromString

instance Convertible [(L8.ByteString, BsonValue)] BsonValue where
    safeConvert = return . BsonDoc . toBsonDoc

instance Convertible [(String, BsonValue)] BsonValue where
    safeConvert = return . BsonDoc . toBsonDoc

instance Convertible [Bool] BsonValue where
    safeConvert bs = BsonArray `liftM` mapM safeConvert bs

instance Convertible [POSIXTime] BsonValue where
    safeConvert ts = BsonArray `liftM` mapM safeConvert ts

instance Convertible [Int] BsonValue where
    safeConvert is = BsonArray `liftM` mapM safeConvert is

instance Convertible [Integer] BsonValue where
    safeConvert is = BsonArray `liftM` mapM safeConvert is

instance Convertible [Int32] BsonValue where
    safeConvert is = BsonArray `liftM` mapM safeConvert is

instance Convertible [Int64] BsonValue where
    safeConvert is = BsonArray `liftM` mapM safeConvert is

instance Convertible POSIXTime BsonValue where
    safeConvert = return . BsonDate

instance Convertible Bool BsonValue where
    safeConvert = return . BsonBool

instance Convertible Int BsonValue where
    safeConvert i = if i >= fromIntegral (minBound::Int32) &&
                       i <= fromIntegral (maxBound::Int32)
                    then return $ BsonInt32 $ fromIntegral i
                    else return $ BsonInt64 $ fromIntegral i

instance Convertible Integer BsonValue where
    safeConvert i = if i >= fromIntegral (minBound::Int32) &&
                       i <= fromIntegral (maxBound::Int32)
                    then return $ BsonInt32 $ fromIntegral i
                    else return $ BsonInt64 $ fromIntegral i

instance Convertible Int32 BsonValue where
    safeConvert = return . BsonInt32

instance Convertible Int64 BsonValue where
    safeConvert = return . BsonInt64

instance (Convertible a BsonValue) =>
    Convertible (Maybe a) BsonValue where
        safeConvert Nothing = return BsonNull
        safeConvert (Just a) = safeConvert a

instance Convertible BsonValue Double where
    safeConvert (BsonDouble d) = return d
    safeConvert (BsonInt32 i) = safeConvert i
    safeConvert (BsonInt64 i) = safeConvert i
    safeConvert v = unsupportedError v

instance Convertible BsonValue Float where
    safeConvert (BsonDouble d) = safeConvert d
    safeConvert (BsonInt32 i) = safeConvert i
    safeConvert (BsonInt64 i) = safeConvert i
    safeConvert v = unsupportedError v

instance Convertible BsonValue String where
    safeConvert (BsonString bs) = return $ L8.toString bs
    safeConvert v = unsupportedError v

instance Convertible BsonValue L8.ByteString where
    safeConvert (BsonString bs) = return bs
    safeConvert v = unsupportedError v

instance Convertible BsonValue S8.ByteString where
    safeConvert (BsonString bs) = return $ C8.concat $ L.toChunks bs
    safeConvert v = unsupportedError v

instance Convertible BsonValue BsonDoc where
    safeConvert (BsonDoc o) = return o
    safeConvert v = unsupportedError v

instance Convertible BsonValue (Map.Map String BsonValue) where
    safeConvert (BsonDoc o) = return $ Map.mapKeys L8.toString o
    safeConvert v = unsupportedError v

instance Convertible BsonValue [(String, BsonValue)] where
    safeConvert (BsonDoc o) = return $ fromBsonDoc o
    safeConvert v = unsupportedError v

instance Convertible BsonValue [(L8.ByteString, BsonValue)] where
    safeConvert (BsonDoc o) = return $ fromBsonDoc o
    safeConvert v = unsupportedError v

instance Convertible BsonValue [Double] where
    safeConvert (BsonArray a) = mapM safeConvert a
    safeConvert v = unsupportedError v

instance Convertible BsonValue [Float] where
    safeConvert (BsonArray a) = mapM safeConvert a
    safeConvert v = unsupportedError v

instance Convertible BsonValue [String] where
    safeConvert (BsonArray a) = mapM safeConvert a
    safeConvert v = unsupportedError v

instance Convertible BsonValue [Bool] where
    safeConvert (BsonArray a) = mapM safeConvert a
    safeConvert v = unsupportedError v

instance Convertible BsonValue [POSIXTime] where
    safeConvert (BsonArray a) = mapM safeConvert a
    safeConvert v = unsupportedError v

instance Convertible BsonValue [Int32] where
    safeConvert (BsonArray a) = mapM safeConvert a
    safeConvert v = unsupportedError v

instance Convertible BsonValue [Int64] where
    safeConvert (BsonArray a) = mapM safeConvert a
    safeConvert v = unsupportedError v

instance Convertible BsonValue Bool where
    safeConvert (BsonBool b) = return b
    safeConvert v = unsupportedError v

instance Convertible BsonValue POSIXTime where
    safeConvert (BsonDate t) = return t
    safeConvert v = unsupportedError v

instance Convertible BsonValue Int where
    safeConvert (BsonDouble d) = safeConvert d
    safeConvert (BsonInt32 d) = safeConvert d
    safeConvert (BsonInt64 d) = safeConvert d
    safeConvert v = unsupportedError v

instance Convertible BsonValue Integer where
    safeConvert (BsonDouble d) = safeConvert d
    safeConvert (BsonInt32 d) = safeConvert d
    safeConvert (BsonInt64 d) = safeConvert d
    safeConvert v = unsupportedError v

instance Convertible BsonValue Int32 where
    safeConvert (BsonDouble d) = safeConvert d
    safeConvert (BsonInt32 d) = return d
    safeConvert (BsonInt64 d) = safeConvert d
    safeConvert v = unsupportedError v

instance Convertible BsonValue Int64 where
    safeConvert (BsonDouble d) = safeConvert d
    safeConvert (BsonInt32 d) = safeConvert d
    safeConvert (BsonInt64 d) = return d
    safeConvert v = unsupportedError v

instance (Convertible BsonValue a) =>
    Convertible (Maybe BsonValue) (Maybe a) where
        safeConvert Nothing = return Nothing
        safeConvert (Just a) = liftM Just $ safeConvert a
