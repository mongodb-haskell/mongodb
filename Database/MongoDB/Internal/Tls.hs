{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

-- | TLS connection to mongodb

module Bend.Database.Mongo.Tls
(
  connect,
)
where

import Data.IORef
import Data.Monoid
import Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Lazy as Lazy.ByteString
import Data.Default.Class (def)
import Control.Exception (bracketOnError)
import System.IO
import Database.MongoDB (Pipe)
import Database.MongoDB.Internal.Protocol (newPipeWith)
import Database.MongoDB.Internal.Connection (Connection(Connection))
import qualified Database.MongoDB.Internal.Connection as Connection
import qualified Network
import qualified Network.TLS as TLS
import qualified Network.TLS.Extra.Cipher as TLS
import qualified Control.IO.Region as Region

-- | Connect to mongodb using TLS
connect :: Text -> Int -> IO Pipe
connect host port = bracketOnError Region.open Region.close $ \r -> do
  handle <- Region.alloc_ r
    (Network.connectTo (Text.unpack host)
                       (Network.PortNumber $ fromIntegral port))
    hClose

  let params = (TLS.defaultParamsClient (Text.unpack host) "")
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
    { Connection.read = do
        rest <- readIORef restRef
        writeIORef restRef mempty
        if ByteString.null rest
          then TLS.recvData ctx
          else return rest
    , Connection.unread = \rest ->
        modifyIORef restRef (rest <>)
    , Connection.write = TLS.sendData ctx . Lazy.ByteString.fromStrict
    , Connection.flush = TLS.contextFlush ctx
    , Connection.close = close
    }
