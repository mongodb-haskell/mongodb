{-# LANGUAGE OverloadedStrings #-}

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
    printDocs "All Teams" =<< allTeams
    printDocs "National League Teams" =<< nationalLeagueTeams
    printDocs "New York Teams" =<< newYorkTeams

clearTeams = delete (select [] "team")

insertTeams = insertMany "team" [
    ["name" =: u"Yankees", "home" =: ["city" =: u"New York", "state" =: u"NY"], "league" =: u"American"],
    ["name" =: u"Mets", "home" =: ["city" =: u"New York", "state" =: u"NY"], "league" =: u"National"],
    ["name" =: u"Phillies", "home" =: ["city" =: u"Philadelphia", "state" =: u"PA"], "league" =: u"National"],
    ["name" =: u"Red Sox", "home" =: ["city" =: u"Boston", "state" =: u"MA"], "league" =: u"American"] ]

allTeams = rest =<< find (select [] "team") {sort = ["home.city" =: (1 :: Int)]}

nationalLeagueTeams = rest =<< find (select ["league" =: u"National"] "team")

newYorkTeams = rest =<< find (select ["home.state" =: u"NY"] "team") {project = ["name" =: (1 :: Int), "league" =: (1 :: Int)]}

printDocs title docs = liftIO $ putStrLn title >> mapM_ (print . exclude ["_id"]) docs
