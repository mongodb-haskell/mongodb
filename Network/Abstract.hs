-- | Generalize a network connection to a sink and source

{-# LANGUAGE MultiParamTypeClasses, ExistentialQuantification, FlexibleContexts, FlexibleInstances, OverlappingInstances, UndecidableInstances #-}

module Network.Abstract where

import System.IO (Handle, hClose)
import Network (HostName, PortID, connectTo)
import Control.Monad.Error
import System.IO.Error (try)
import Control.Monad.Context
import Control.Monad.Util (MonadIO')

type IOE = ErrorT IOError IO
-- ^ Be explicit about exception that may be raised.

data Server i o = Server HostName PortID
-- ^ A server receives messages of type i and returns messages of type o.

-- | Serialize message over handle
class WriteMessage i where
	writeMessage :: Handle -> i -> IOE ()

-- | Deserialize message from handle
class ReadMessage o where
	readMessage :: Handle -> IOE o

-- | A network controls connections to other hosts. It may want to overide to log messages or play them back.
class Network n where
	connect :: (WriteMessage i, ReadMessage o) => n -> Server i o -> IOE (Connection i o)
	-- ^ Connect to Server returning the send sink and receive source, throw IOError if can't connect.

data Connection i o = Connection {
	send :: i -> IOE (),
	receive :: IOE o,
	close :: IO () }

data ANetwork = forall n. (Network n) => ANetwork n

instance Network (ANetwork) where
	connect (ANetwork n) = connect n

data Internet = Internet
-- ^ Normal Network instance, i.e. no logging or replay

-- | Connect to server. Write messages and receive replies. Not thread-safe, must be wrapped in Pipeline or something.
instance Network Internet where
	connect _ (Server hostname portid) = ErrorT . try $ do
		handle <- connectTo hostname portid
		return $ Connection (writeMessage handle) (readMessage handle) (hClose handle)

class (MonadIO' m) => NetworkIO m where
	network :: m ANetwork

instance (Context ANetwork m, MonadIO' m) => NetworkIO m where
	network = context

instance NetworkIO IO where
	network = return (ANetwork Internet)

{- Authors: Tony Hannan <tony@10gen.com>
   Copyright 2010 10gen Inc.
   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. -}
