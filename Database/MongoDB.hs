{- |
Client interface to MongoDB database management system.

Simple example below. Use with language extension /OvererloadedStrings/.

> {-# LANGUAGE OverloadedStrings #-}
>
> import Database.MongoDB
> import Control.Monad.Trans (liftIO)
>
> main = do
>    pipe <- runIOE $ connect (host "127.0.0.1")
>    e <- access pipe master "baseball" run
>    close pipe
>    print e
>
> run = do
>    clearTeams
>    insertTeams
>    allTeams >>= printDocs "All Teams"
>    nationalLeagueTeams >>= printDocs "National League Teams"
>    newYorkTeams >>= printDocs "New York Teams"
>
> clearTeams = delete (select [] "team")
>
> insertTeams = insertMany "team" [
>    ["name" =: u"Yankees", "home" =: ["city" =: u"New York", "state" =: u"NY"], "league" =: u"American"],
>    ["name" =: u"Mets", "home" =: ["city" =: u"New York", "state" =: u"NY"], "league" =: u"National"],
>    ["name" =: u"Phillies", "home" =: ["city" =: u"Philadelphia", "state" =: u"PA"], "league" =: u"National"],
>    ["name" =: u"Red Sox", "home" =: ["city" =: u"Boston", "state" =: u"MA"], "league" =: u"American"] ]
>
> allTeams = rest =<< find (select [] "team") {sort = ["home.city" =: (1 :: Int)]}
>
> nationalLeagueTeams = rest =<< find (select ["league" =: u"National"] "team")
>
> newYorkTeams = rest =<< find (select ["home.state" =: u"NY"] "team") {project = ["name" =: (1 :: Int), "league" =: (1 :: Int)]}
>
> printDocs title docs = liftIO $ putStrLn title >> mapM_ (print . exclude ["_id"]) docs
>
-}

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


{- Authors: Tony Hannan <tony@10gen.com>
   Copyright 2010-11 10gen Inc.
   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at: http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. -}
