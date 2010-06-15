-- | Client interface to MongoDB server(s)

module Database.MongoDB (
	module Data.Bson,
	module Database.MongoDB.Connection,
	module Database.MongoDB.Query,
	module Database.MongoDB.Admin
) where

import Data.Bson
import Database.MongoDB.Connection
import Database.MongoDB.Query
import Database.MongoDB.Admin
