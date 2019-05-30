-- | Compatibility layer for network package, including newtype 'PortID'
{-# LANGUAGE CPP, PackageImports #-}

module Database.MongoDB.Internal.Network (PortID(..), N.HostName, connectTo) where

import Control.Exception (bracketOnError)
import Network.BSD as BSD
import System.IO (Handle, IOMode(ReadWriteMode))

#if !MIN_VERSION_network(2, 8, 0)
import qualified Network as N
#else
import qualified Network.Socket as N
#endif

newtype PortID = PortNumber N.PortNumber deriving (Show, Eq, Ord)

-- Taken from network 2.8's connectTo
-- https://github.com/haskell/network/blob/e73f0b96c9da924fe83f3c73488f7e69f712755f/Network.hs#L120-L129
connectTo :: N.HostName         -- Hostname
          -> PortID             -- Port Identifier
          -> IO Handle          -- Connected Socket
connectTo hostname (PortNumber port) = do
    proto <- BSD.getProtocolNumber "tcp"
    bracketOnError
        (N.socket N.AF_INET N.Stream proto)
        (N.close)  -- only done if there's an error
        (\sock -> do
          he <- BSD.getHostByName hostname
          N.connect sock (N.SockAddrInet port (hostAddress he))
          N.socketToHandle sock ReadWriteMode
        )
