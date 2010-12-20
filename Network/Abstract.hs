-- | Generalize a network connection to a sink and source

{-# LANGUAGE MultiParamTypeClasses, ExistentialQuantification, FlexibleContexts, FlexibleInstances, UndecidableInstances #-}

module Network.Abstract where

import Network (HostName, PortID)
import Control.Monad.Error

type IOE = ErrorT IOError IO

type Server = (HostName, PortID)

-- | A network controls connections to other hosts. It may want to overide to log messages or play them back.
-- A server in the network accepts messages of type i and generates messages of type o.
class Network n i o where
	connect :: n -> Server -> IOE (Connection i o)
	-- ^ Connect to Server returning the send sink and receive source, throw IOError if can't connect.

data Connection i o = Connection {
	send :: i -> IOE (),
	receive :: IOE o,
	close :: IO () }

data ANetwork i o = forall n. (Network n i o) => ANetwork n

instance Network (ANetwork i o) i o where
	connect (ANetwork n) = connect n


{- Authors: Tony Hannan <tony@10gen.com>
   Copyright 2010 10gen Inc.
   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. -}
