{- |
Client interface to MongoDB server(s).

Simple example below. Use with language extension /OvererloadedStrings/.

> {-# LANGUAGE OverloadedStrings #-}
>
> import Database.MongoDB
> import Data.UString (u)
> import Control.Monad.Trans (liftIO)
>
> main = do
>    ee <- runNet $ do
>       conn <- connect (host "127.0.0.1")
>       runConn run conn
>    print ee
>
> run = useDb "baseball" $ do
>    clearTeams
>    insertTeams
>    print' "All Teams" =<< allTeams
>    print' "National League Teams" =<< nationalLeagueTeams
>    print' "New York Teams" =<< newYorkTeams
>
> clearTeams = delete (select [] "team")
>
> insertTeams = insertMany "team" [
>    ["name" =: u"Yankees", "home" =: ["city" =: u"New York", "state" =: u"NY"], "league" =: u"American"],
>    ["name" =: u"Mets", "home" =: ["city" =: u"New York", "state" =: u"NY"], "league" =: u"National"],
>    ["name" =: u"Phillies", "home" =: ["city" =: u"Philadelphia", "state" =: u"PA"], "league" =: u"National"],
>    ["name" =: u"Red Sox", "home" =: ["city" =: u"Boston", "state" =: u"MA"], "league" =: u"American"] ]
>
> allTeams = rest =<< find (select [] "team") {sort = ["city" =: (1 :: Int)]}
>
> nationalLeagueTeams = rest =<< find (select ["league" =: u"National"] "team")
>
> newYorkTeams = rest =<< find (select ["home.state" =: u"NY"] "team") {project = ["name" =: (1 :: Int), "league" =: (1 :: Int)]}
>
> print' title docs = liftIO $ putStrLn title >> mapM_ print docs
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
