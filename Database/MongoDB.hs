{- |
Client interface to MongoDB database management system.

Simple example below. Use with language extensions /OvererloadedStrings/ & /ExtendedDefaultRules/.

> {-# LANGUAGE OverloadedStrings, ExtendedDefaultRules #-}
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
>    ["name" =: "Yankees", "home" =: ["city" =: "New York", "state" =: "NY"], "league" =: "American"],
>    ["name" =: "Mets", "home" =: ["city" =: "New York", "state" =: "NY"], "league" =: "National"],
>    ["name" =: "Phillies", "home" =: ["city" =: "Philadelphia", "state" =: "PA"], "league" =: "National"],
>    ["name" =: "Red Sox", "home" =: ["city" =: "Boston", "state" =: "MA"], "league" =: "American"] ]
>
> allTeams = rest =<< find (select [] "team") {sort = ["home.city" =: 1]}
>
> nationalLeagueTeams = rest =<< find (select ["league" =: "National"] "team")
>
> newYorkTeams = rest =<< find (select ["home.state" =: "NY"] "team") {project = ["name" =: 1, "league" =: 1]}
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
