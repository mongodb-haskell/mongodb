{- |
Client interface to MongoDB server(s).

Simple example:

>
> {-# LANGUAGE OverloadedStrings #-}
>
> import Database.MongoDB
>
> main = do
>    e <- connect (server "127.0.0.1")
>    conn <- either (fail . show) return e
>    e <- runConn run conn
>    either (fail . show) return e
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
>    ["name" =: "Yankees", "home" =: ["city" =: "New York", "state" =: "NY"], "league" =: "American"],
>    ["name" =: "Mets", "home" =: ["city" =: "New York", "state" =: "NY"], "league" =: "National"],
>    ["name" =: "Phillies", "home" =: ["city" =: "Philadelphia", "state" =: "PA"], "league" =: "National"],
>    ["name" =: "Red Sox", "home" =: ["city" =: "Boston", "state" =: "MA"], "league" =: "American"] ]
>
> allTeams = rest =<< find (select [] "team") {sort = ["city" =: 1]}
>
> nationalLeagueTeams = rest =<< find (select ["league" =: "National"] "team")
>
> newYorkTeams = rest =<< find (select ["home.state" =: "NY"] "team") {project = ["name" =: 1, "league" =: 1]}
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
