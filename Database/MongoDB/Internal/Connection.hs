
-- | This module defines a connection interface. It could be a regular
-- network connection, TLS connection, a mock or anything else.

module Database.MongoDB.Internal.Connection (
    Connection(..),
    readExactly,
    fromHandle,
) where

import Prelude hiding (read)
import Data.Monoid
import Data.IORef
import Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Lazy as Lazy (ByteString)
import qualified Data.ByteString.Lazy as Lazy.ByteString
import Control.Monad
import System.IO
import System.IO.Error (mkIOError, eofErrorType)

-- | Abstract connection interface
--
-- `read` should return `ByteString.null` on EOF
data Connection = Connection {
    readExactly :: Int -> IO ByteString,
    write :: ByteString -> IO (),
    flush :: IO (),
    close :: IO ()}

fromHandle :: Handle -> IO Connection
-- ^ Make connection form handle
fromHandle handle = do
  return Connection
    { readExactly = ByteString.hGet handle
    , write = ByteString.hPut handle
    , flush = hFlush handle
    , close = hClose handle
    }
