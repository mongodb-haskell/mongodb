
import Control.Monad
import Data.Binary
import Data.Binary.Get
import Data.Binary.IEEE754
import Data.Binary.Put
import Data.Bits
import Data.ByteString.Char8
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy.Char8 as LC
import qualified Data.ByteString.Lazy.UTF8 as L8
import Data.Char          (chr, ord)
import Data.Int
import qualified Data.List as List
import Data.Time.Clock.POSIX

data BSValue
    = BSDouble Double
    | BSString L8.ByteString
    | BSObject BSONObject
    | BSArray [BSValue]
    | BSUndefined
    | BSBinary BinarySubType L.ByteString
    | BSObjectId L.ByteString
    | BSBool !Bool
    | BSDate POSIXTime
    | BSNull
    | BSRegex L8.ByteString String
    | BSSymbol L8.ByteString
    | BSInt32 Int32
    | BSInt64 Int64
    | BSMinKey
    | BSMaxKey
    deriving (Show, Eq, Ord)

newtype BSONObject = BSONObject { fromBSONObject :: [(L8.ByteString, BSValue)] }
    deriving (Eq, Ord, Show)

toBSONObject :: [(L8.ByteString, BSValue)] -> BSONObject
toBSONObject = BSONObject

data DataType =
    Data_min_key        | -- -1
    Data_number         | -- 1
    Data_string         | -- 2
    Data_object	        | -- 3
    Data_array          | -- 4
    Data_binary         | -- 5
    Data_undefined      | -- 6
    Data_oid            | -- 7
    Data_boolean        | -- 8
    Data_date           | -- 9
    Data_null           | -- 10
    Data_regex          | -- 11
    Data_ref            | -- 12
    Data_code           | -- 13
    Data_symbol	        | -- 14
    Data_code_w_scope   | -- 15
    Data_int            | -- 16
    Data_timestamp      | -- 17
    Data_long           | -- 18
    Data_max_key          -- 127
    deriving (Show, Read, Enum, Eq, Ord)

toDataType :: Int -> DataType
toDataType (-1) = Data_min_key
toDataType 127 = Data_max_key
toDataType d = toEnum d

fromDataType :: DataType -> Int
fromDataType Data_min_key = (-1)
fromDataType Data_max_key = 127
fromDataType d = fromEnum d


data BinarySubType =
    BSTUNDEFINED_1     |
    BSTFunction        | -- 1
    BSTByteArray       | -- 2
    BSTUUID            | -- 3
    BSTUNDEFINED_2     |
    BSTMD5             | -- 5
    BSTUserDefined
    deriving (Show, Read, Enum, Eq, Ord)

toBinarySubType :: Int -> BinarySubType
toBinarySubType 0x80 = BSTUserDefined
toBinarySubType d = toEnum d

fromBinarySubType :: BinarySubType -> Int
fromBinarySubType BSTUserDefined = 0x80
fromBinarySubType d = fromEnum d

instance Binary BSONObject where
    get = liftM snd getObj
    put = putObj

getVal :: DataType -> Get (Int, BSValue)
getVal Data_number = getFloat64le >>= return . (,) 8 . BSDouble
getVal Data_string = do
  sLen1 <- getI32
  (sLen2, s) <- getS
  return (fromIntegral $ 4 + sLen1, BSString s)
getVal Data_object = getObj >>= \(len, obj) -> return (len, BSObject obj)
getVal Data_array = do
  (len, arr) <- getRawObj
  let arr2 = List.foldl (\acc (k,v) -> v : acc) [] arr -- reverse and remove key
  return (len, BSArray arr2)
getVal Data_binary = do
  skip 4
  st   <- getI8
  len2 <- getI32
  bs   <- getLazyByteString $ fromIntegral len2
  return (4 + 1 + 4 + len2, BSBinary (toBinarySubType st) bs)
getVal Data_undefined = return (1, BSUndefined)
getVal Data_oid = getLazyByteString 12 >>= return . (,) 12 . BSObjectId
getVal Data_boolean = getI8 >>= return . (,) 1 . BSBool . (/= 0)
getVal Data_date =
    getI64 >>= return . (,) 8 . BSDate . flip (/) 1000 . realToFrac
getVal Data_null = return (1, BSNull)
getVal Data_int = getI32 >>= return . (,) 4 . BSInt32 . fromIntegral
getVal Data_long = getI64 >>= return . (,) 8 . BSInt64

getInnerObj 1 obj = obj
getInnerObj bytesLeft obj = do
  typ <- getDataType
  (keySz, key) <- getS
  (valSz, val) <- getVal typ
  getInnerObj (bytesLeft - 1 - keySz - valSz) $ liftM ((key, val) :) obj

getRawObj = do
  bytes <- getI32
  obj <- getInnerObj (bytes - 4) $ return []
  getNull
  return (bytes, obj)

getObj = getRawObj >>= \(len, obj) ->  return (len, toBSONObject obj)

getS :: Get (Int, L8.ByteString)
getS = getLazyByteStringNul >>= \s -> return (fromIntegral $ L.length s + 1, s)

getNull = do {'\0' <- getC; return ()}

getC = liftM chr getI8
getI8 = liftM fromIntegral getWord8
getI32 = liftM fromIntegral getWord32le
getI64 = liftM fromIntegral getWord64le
getDataType = liftM toDataType getI8


putType BSDouble{}   = putDataType Data_number
putType BSString{}   = putDataType Data_string
putType BSObject{}   = putDataType Data_object
putType BSArray{}    = putDataType Data_array
putType BSBinary{}   = putDataType Data_binary
putType BSUndefined  = putDataType Data_undefined
putType BSObjectId{} = putDataType Data_oid
putType BSBool{}     = putDataType Data_boolean
putType BSDate{}     = putDataType Data_date
putType BSNull       = putDataType Data_null
putType BSRegex{}    = putDataType Data_regex
-- putType = putDataType Data_ref
-- putType = putDataType Data_code
putType BSSymbol{}   = putDataType Data_symbol
-- putType = putDataType Data_code_w_scope
putType (BSInt32 i)  = putDataType Data_int
putType (BSInt64 i)  = putDataType Data_long
-- putType = putDataType Data_timestamp
putType BSMinKey     = putDataType Data_min_key
putType BSMaxKey     = putDataType Data_max_key


putVal (BSDouble d)   = putFloat64le d
putVal (BSString s)   = putI32 (1 + L8.length s) >> putS s
putVal (BSObject o)   = putObj o
putVal (BSArray es)   = putOutterObj bs
    where bs = runPut $ forM_ (List.zip [0..] es) $ \(i, e) ->
               putType e >> (putS $ L8.fromString $ show i) >> putVal e
putVal (BSBinary t bs)= do putI32 $ 4 + L.length bs
                           putI8 $ fromBinarySubType t
                           putI32 $ L.length bs
                           putLazyByteString bs
putVal BSUndefined    = flush
putVal (BSObjectId o) = putLazyByteString o
putVal (BSBool False) = putI8 0
putVal (BSBool True)  = putI8 1
putVal (BSDate pt)    = putI64 $ round $ 1000 * realToFrac pt
putVal BSNull         = flush
putVal (BSRegex r opt)= do putS r
                           putByteString $ pack $ List.sort opt
                           putNull
putVal (BSSymbol s)   = putI32 (1 + L8.length s) >> putS s
putVal (BSInt32 i)    = putI32 i
putVal (BSInt64 i)    = putI64 i
putVal BSMinKey       = flush
putVal BSMaxKey       = flush

putObj obj   = putOutterObj bs
    where bs = runPut $ forM_ (fromBSONObject obj) $ \(k, v) ->
               putType v >> putS k >> putVal v

putOutterObj bytes = do
  -- the length prefix and null term are included in the length
  putI32 $ 4 + 1 + L.length bytes
  putLazyByteString bytes
  putNull

putNull = putI8 0
putI8 :: (Integral i) => i -> Put
putI8 = putWord8 . fromIntegral

putI32 :: (Integral i) => i -> Put
putI32 = putWord32le . fromIntegral

putI64 :: (Integral i) => i -> Put
putI64 = putWord64le . fromIntegral

putS :: L8.ByteString -> Put
putS s = putLazyByteString s >> putNull

putDataType = putI8 . fromDataType

int32_min = -2147483648
int32_max = -int32_min - 1
