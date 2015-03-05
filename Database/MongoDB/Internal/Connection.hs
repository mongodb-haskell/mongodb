
-- | This module defines a connection interface. It could be a regular
-- network connection, TLS connection, a mock or anything else.

module Database.MongoDB.Internal.Connection (
    Connection(..),
    readExactly,
    writeLazy,
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
    read :: IO ByteString,
    unread :: ByteString -> IO (),
    write :: ByteString -> IO (),
    flush :: IO (),
    close :: IO ()}

readExactly :: Connection -> Int -> IO Lazy.ByteString
-- ^ Read specified number of bytes
--
-- If EOF is reached before N bytes then raise EOF exception.
readExactly conn count = go mempty count
  where
  go acc n = do
    -- read until get enough bytes
    chunk <- read conn
    when (ByteString.null chunk) $
      ioError eof
    let len = ByteString.length chunk
    if len >= n
      then do
        let (res, rest) = ByteString.splitAt n chunk
        unless (ByteString.null rest) $
          unread conn rest
        return (acc <> Lazy.ByteString.fromStrict res)
      else go (acc <> Lazy.ByteString.fromStrict chunk) (n - len)
  eof = mkIOError eofErrorType "Database.MongoDB.Internal.Connection"
                  Nothing Nothing

writeLazy :: Connection -> Lazy.ByteString -> IO ()
writeLazy conn = mapM_ (write conn) . Lazy.ByteString.toChunks

fromHandle :: Handle -> IO Connection
-- ^ Make connection form handle
fromHandle handle = do
  restRef <- newIORef mempty
  return Connection
    { read = do
        rest <- readIORef restRef
        writeIORef restRef mempty
        if ByteString.null rest
          -- 32k corresponds to the default chunk size
          -- used in bytestring package
          then ByteString.hGetSome handle (32 * 1024)
          else return rest
    , unread = \rest ->
        modifyIORef restRef (rest <>)
    , write = ByteString.hPut handle
    , flush = hFlush handle
    , close = hClose handle
    }
