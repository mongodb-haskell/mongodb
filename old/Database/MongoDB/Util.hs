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

module Database.MongoDB.Util
    (
     putI8, putI16, putI32, putI64, putNothing, putNull, putS,
     getI8, getI32, getI64, getC, getS, getNull, putStrSz,
    )
where
import Control.Exception (assert)
import Control.Monad
import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.ByteString.Char8
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy.UTF8 as L8
import Data.Char          (chr)
import Data.Int

getC :: Get Char
getC = liftM chr getI8

getI8 :: (Integral a) => Get a
getI8 = liftM fromIntegral getWord8

getI32 :: Get Int32
getI32 = liftM fromIntegral getWord32le

getI64 :: Get Int64
getI64 = liftM fromIntegral getWord64le

getS :: Get (Integer, L8.ByteString)
getS = getLazyByteStringNul >>= \s -> return (fromIntegral $ L.length s + 1, s)

getNull :: Get ()
getNull = do {c <- getC; assert (c == '\0') $ return ()}

putI8 :: Int8 -> Put
putI8 = putWord8 . fromIntegral

putI16 :: Int16 -> Put
putI16 = putWord16le . fromIntegral

putI32 :: Int32 -> Put
putI32 = putWord32le . fromIntegral

putI64 :: Int64 -> Put
putI64 = putWord64le . fromIntegral

putNothing :: Put
putNothing = putByteString $ pack ""

putNull :: Put
putNull = putI8 0

putS :: L8.ByteString -> Put
putS s = putLazyByteString s >> putNull

putStrSz :: L8.ByteString -> Put
putStrSz s = putI32 (fromIntegral $ 1 + L.length s) >> putS s
