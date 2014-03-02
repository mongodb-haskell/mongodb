{-# LANGUAGE OverloadedStrings, ExtendedDefaultRules #-}

import Database.MongoDB
import Control.Monad.Trans (liftIO)

main :: IO ()
main = do
    pipe <- runIOE $ connect (host "127.0.0.1")
    e <- access pipe master "baseball" run
    close pipe
    print e

run :: Action IO ()
run = do
    clearTeams
    insertTeams
    allTeams >>= printDocs "All Teams"
    nationalLeagueTeams >>= printDocs "National League Teams"
    newYorkTeams >>= printDocs "New York Teams"

clearTeams :: Action IO ()
clearTeams = delete (select [] "team")

insertTeams :: Action IO [Value]
insertTeams = insertMany "team" [
    ["name" =: "Yankees", "home" =: ["city" =: "New York", "state" =: "NY"], "league" =: "American"],
    ["name" =: "Mets", "home" =: ["city" =: "New York", "state" =: "NY"], "league" =: "National"],
    ["name" =: "Phillies", "home" =: ["city" =: "Philadelphia", "state" =: "PA"], "league" =: "National"],
    ["name" =: "Red Sox", "home" =: ["city" =: "Boston", "state" =: "MA"], "league" =: "American"] ]

allTeams :: Action IO [Document]
allTeams = rest =<< find (select [] "team") {sort = ["home.city" =: 1]}

nationalLeagueTeams :: Action IO [Document]
nationalLeagueTeams = rest =<< find (select ["league" =: "National"] "team")

newYorkTeams :: Action IO [Document]
newYorkTeams = rest =<< find (select ["home.state" =: "NY"] "team") {project = ["name" =: 1, "league" =: 1]}

printDocs :: String -> [Document] -> Action IO ()
printDocs title docs = liftIO $ putStrLn title >> mapM_ (print . exclude ["_id"]) docs
