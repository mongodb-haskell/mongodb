{-# LANGUAGE OverloadedStrings, ExtendedDefaultRules #-}

import Database.MongoDB
import Control.Monad.Trans (liftIO)

main = do
    pipe <- runIOE $ connect (host "127.0.0.1")
    e <- access pipe master "baseball" run
    close pipe
    print e

run = do
    clearTeams
    insertTeams
    allTeams >>= printDocs "All Teams"
    nationalLeagueTeams >>= printDocs "National League Teams"
    newYorkTeams >>= printDocs "New York Teams"

clearTeams = delete (select [] "team")

insertTeams = insertMany "team" [
    ["name" =: "Yankees", "home" =: ["city" =: "New York", "state" =: "NY"], "league" =: "American"],
    ["name" =: "Mets", "home" =: ["city" =: "New York", "state" =: "NY"], "league" =: "National"],
    ["name" =: "Phillies", "home" =: ["city" =: "Philadelphia", "state" =: "PA"], "league" =: "National"],
    ["name" =: "Red Sox", "home" =: ["city" =: "Boston", "state" =: "MA"], "league" =: "American"] ]

allTeams = rest =<< find (select [] "team") {sort = ["home.city" =: 1]}

nationalLeagueTeams = rest =<< find (select ["league" =: "National"] "team")

newYorkTeams = rest =<< find (select ["home.state" =: "NY"] "team") {project = ["name" =: 1, "league" =: 1]}

printDocs title docs = liftIO $ putStrLn title >> mapM_ (print . exclude ["_id"]) docs
