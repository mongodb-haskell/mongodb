{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

-- | TLS connection to mongodb

module Database.MongoDB.Internal.Tls
(
  connect,
)
where

import Data.IORef
import Data.Monoid
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Lazy as Lazy.ByteString
import Data.Default.Class (def)
import Control.Applicative ((<$>))
import Control.Exception (bracketOnError)
import Control.Monad (when, unless)
import System.IO
import Database.MongoDB (Pipe)
import Database.MongoDB.Internal.Protocol (newPipeWith)
import Database.MongoDB.Internal.Connection (Connection(Connection))
import qualified Database.MongoDB.Internal.Connection as Connection
import System.IO.Error (mkIOError, eofErrorType)
import Network (connectTo, HostName, PortID)
import qualified Network.TLS as TLS
import qualified Network.TLS.Extra.Cipher as TLS
import qualified Control.IO.Region as Region

-- | Connect to mongodb using TLS
connect :: HostName -> PortID -> IO Pipe
connect host port = bracketOnError Region.open Region.close $ \r -> do
  handle <- Region.alloc_ r
    (connectTo host port)
    hClose

  let params = (TLS.defaultParamsClient host "")
        { TLS.clientSupported = def
            { TLS.supportedCiphers = TLS.ciphersuite_all}
        , TLS.clientHooks = def
            { TLS.onServerCertificate = \_ _ _ _ -> return []}
        }
  context <- Region.alloc_ r
    (TLS.contextNew handle params)
    TLS.contextClose
  TLS.handshake context

  conn <- tlsConnection context (Region.close r)
  newPipeWith conn

tlsConnection :: TLS.Context -> IO () -> IO Connection
tlsConnection ctx close = do
  restRef <- newIORef mempty
  return Connection
    { Connection.read = \count -> let
          readSome = do
            rest <- readIORef restRef
            writeIORef restRef mempty
            if ByteString.null rest
              then TLS.recvData ctx
              else return rest
          unread = \rest ->
            modifyIORef restRef (rest <>)
          go acc n = do
            -- read until get enough bytes
            chunk <- readSome
            when (ByteString.null chunk) $
              ioError eof
            let len = ByteString.length chunk
            if len >= n
              then do
                let (res, rest) = ByteString.splitAt n chunk
                unless (ByteString.null rest) $
                  unread rest
                return (acc <> Lazy.ByteString.fromStrict res)
              else go (acc <> Lazy.ByteString.fromStrict chunk) (n - len)
          eof = mkIOError eofErrorType "Database.MongoDB.Internal.Connection"
                Nothing Nothing
       in Lazy.ByteString.toStrict <$> go mempty count
    , Connection.write = TLS.sendData ctx . Lazy.ByteString.fromStrict
    , Connection.flush = TLS.contextFlush ctx
    , Connection.close = close
    }
