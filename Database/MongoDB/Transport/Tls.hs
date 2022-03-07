{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

#if (__GLASGOW_HASKELL__ >= 706)
{-# LANGUAGE RecursiveDo #-}
#else
{-# LANGUAGE DoRec #-}
#endif

{-|
Module      : MongoDB TLS
Copyright   : (c)	Yuras Shumovich, 2016
License     : Apache 2.0
Maintainer  : Victor Denisov denisovenator@gmail.com
Stability   : experimental
Portability : POSIX

This module is for connecting to TLS enabled mongodb servers.
ATTENTION!!! Be aware that this module is highly experimental and is
barely tested. The current implementation doesn't verify server's identity.
It only allows you to connect to a mongodb server using TLS protocol.
-}
module Database.MongoDB.Transport.Tls
( connect
, connectWithTlsParams
)
where

import Data.IORef
import Data.Monoid
import Data.Maybe(fromMaybe)
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Lazy as Lazy.ByteString
import Data.Default.Class (def)
import Control.Applicative ((<$>))
import Control.Exception (bracketOnError)
import Control.Monad (when, unless)
import System.IO
import Database.MongoDB.Internal.Protocol (Pipe, newPipeWith)
import Database.MongoDB.Transport (Transport(Transport))
import qualified Database.MongoDB.Transport as T
import System.IO.Error (mkIOError, eofErrorType)
import Database.MongoDB.Internal.Network (connectTo, HostName, PortID)
import qualified Network.TLS as TLS
import qualified Network.TLS.Extra.Cipher as TLS
import Database.MongoDB.Query (access, slaveOk, retrieveServerData)

-- | Connect to mongodb using TLS
connect :: HostName -> PortID -> IO Pipe
connect host port = connectWithTlsParams params host port
  where
    params = (TLS.defaultParamsClient host "")
        { TLS.clientSupported = def
            { TLS.supportedCiphers = TLS.ciphersuite_default }
        , TLS.clientHooks = def
            { TLS.onServerCertificate = \_ _ _ _ -> return [] }
        }

-- | Connect to mongodb using TLS using provided TLS client parameters
connectWithTlsParams :: TLS.ClientParams -> HostName -> PortID -> IO Pipe
connectWithTlsParams clientParams host port = bracketOnError (connectTo host port) hClose $ \handle -> do
  context <- TLS.contextNew handle clientParams
  TLS.handshake context

  conn <- tlsConnection context
  rec
    p <- newPipeWith sd conn
    sd <- access p slaveOk "admin" retrieveServerData
  return p


tlsConnection :: TLS.Context -> IO Transport
tlsConnection ctx = do
  restRef <- newIORef mempty
  return Transport
    { T.read = \count -> let
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
          eof = mkIOError eofErrorType "Database.MongoDB.Transport"
                Nothing Nothing
       in Lazy.ByteString.toStrict <$> go mempty count
    , T.write = TLS.sendData ctx . Lazy.ByteString.fromStrict
    , T.flush = TLS.contextFlush ctx
    , T.close = TLS.contextClose ctx
    }
