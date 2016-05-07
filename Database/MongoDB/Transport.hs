
-- | This module defines a connection interface. It could be a regular
-- network connection, TLS connection, a mock or anything else.

module Database.MongoDB.Transport (
    Transport(..),
    fromHandle,
) where

import Prelude hiding (read)
import Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import System.IO

-- | Abstract transport interface
--
-- `read` should return `ByteString.null` on EOF
data Transport = Transport {
    read  :: Int -> IO ByteString,
    write :: ByteString -> IO (),
    flush :: IO (),
    close :: IO ()}

fromHandle :: Handle -> IO Transport
-- ^ Make connection form handle
fromHandle handle = do
  return Transport
    { read  = ByteString.hGet handle
    , write = ByteString.hPut handle
    , flush = hFlush handle
    , close = hClose handle
    }
