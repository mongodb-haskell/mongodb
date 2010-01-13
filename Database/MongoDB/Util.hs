module Database.MongoDB.Util
    (
     putI8, putI32, putI64, putNothing, putNull, putS,
     getI8, getI32, getI64, getC, getS, getNull,
    )
where
import Control.Monad
import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.ByteString.Char8
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy.UTF8 as L8
import Data.Char          (chr, ord)
import Data.Int

getC = liftM chr getI8
getI8 = liftM fromIntegral getWord8

getI32 :: Get Int32
getI32 = liftM fromIntegral getWord32le

getI64 :: Get Int64
getI64 = liftM fromIntegral getWord64le

getS :: Get (Integer, L8.ByteString)
getS = getLazyByteStringNul >>= \s -> return (fromIntegral $ L.length s + 1, s)

getNull = do {'\0' <- getC; return ()}

putI8 :: (Integral i) => i -> Put
putI8 = putWord8 . fromIntegral

putI32 :: Int32 -> Put
putI32 = putWord32le . fromIntegral

putI64 :: Int64 -> Put
putI64 = putWord64le . fromIntegral

putNothing = putByteString $ pack ""

putNull = putI8 0

putS :: L8.ByteString -> Put
putS s = putLazyByteString s >> putNull
