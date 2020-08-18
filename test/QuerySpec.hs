{-# LANGUAGE OverloadedStrings, ExtendedDefaultRules, ScopedTypeVariables #-}
{-# OPTIONS_GHC -fno-warn-type-defaults #-}

module QuerySpec (spec) where
import Data.String (IsString(..))
import TestImport
import Control.Exception
import Control.Monad (forM_, when)
import System.Environment (getEnv)
import System.IO.Error (catchIOError)
import qualified Data.List as L

import qualified Data.Text as T

testDBName :: Database
testDBName = "mongodb-haskell-test"

db :: Action IO a -> IO a
db action = do
    mongodbHost <- getEnv mongodbHostEnvVariable `catchIOError` (\_ -> return "localhost")
    pipe <- connect (host mongodbHost)
    result <- access pipe master testDBName action
    close pipe
    return result

getWireVersion :: IO Int
getWireVersion = db $ do
    sd <- retrieveServerData
    return $ maxWireVersion sd

withCleanDatabase :: ActionWith () -> IO ()
withCleanDatabase action = dropDB >> action () >> dropDB >> return ()
  where
    dropDB = db $ dropDatabase testDBName

insertDuplicateWith :: (Collection -> [Document] -> Action IO a) -> IO ()
insertDuplicateWith testInsert = do
  _id <- db $ insert "team" ["name" =: "Dodgers", "league" =: "American"]
  _ <- db $ testInsert "team" [ ["name" =: "Yankees", "league" =: "American"]
                              -- Try to insert document with duplicate key
                              , ["name" =: "Dodgers", "league" =: "American", "_id" =: _id]
                              , ["name" =: "Indians", "league" =: "American"]
                              ]
  return ()

insertUsers :: IO ()
insertUsers = db $
  insertAll_ "users" [ ["_id" =: "jane", "joined" =: parseDate "2011-03-02", "likes" =: ["golf", "racquetball"]]
                     , ["_id" =: "joe", "joined" =: parseDate "2012-07-02", "likes" =: ["tennis", "golf", "swimming"]]
                     , ["_id" =: "jill", "joined" =: parseDate "2013-11-17", "likes" =: ["cricket", "golf"]]
                     ]

pendingIfMongoVersion :: ((Integer, Integer) -> Bool) -> SpecWith () -> Spec
pendingIfMongoVersion invalidVersion = before $ do
  version <- db $ extractVersion . T.splitOn "." . at "version" <$> runCommand1 "buildinfo"
  when (invalidVersion version) $ pendingWith "This test does not run in the current database version"
  where
    extractVersion (major:minor:_) = (read $ T.unpack major, read $ T.unpack minor)
    extractVersion _               = error "Invalid version specification"

bigDocument :: Document
bigDocument = (flip map) [1..10000] $ \i -> (fromString $ "team" ++ (show i)) =: ("team " ++ (show i) ++ " name")

fineGrainedBigDocument :: Document
fineGrainedBigDocument = (flip map) [1..1000] $ \i -> (fromString $ "team" ++ (show i)) =: ("team " ++ (show i) ++ " name")

hugeDocument :: Document
hugeDocument = (flip map) [1..1000000] $ \i -> (fromString $ "team" ++ (show i)) =: ("team " ++ (show i) ++ " name")

spec :: Spec
spec = around withCleanDatabase $ do
  describe "useDb" $ do
    it "changes the db" $ do
      let anotherDBName = "another-mongodb-haskell-test"
      db thisDatabase `shouldReturn` testDBName
      db (useDb anotherDBName thisDatabase) `shouldReturn` anotherDBName

  describe "insert" $ do
    it "inserts a document to the collection and returns its _id" $ do
      _id <- db $ insert "team" ["name" =: "Yankees", "league" =: "American"]
      result <- db $ rest =<< find (select [] "team")
      result `shouldBe` [["_id" =: _id, "name" =: "Yankees", "league" =: "American"]]

  describe "insert_" $ do
    it "inserts a document to the collection and doesn't return _id" $ do
      _id <- db $ insert_ "team" ["name" =: "Yankees", "league" =: "American"]
      db (count $ select ["name" =: "Yankees", "league" =: "American"] "team") `shouldReturn` 1
      _id `shouldBe` ()

  describe "insertMany" $ do
    it "inserts documents to the collection and returns their _ids" $ do
      (_id1:_id2:_) <- db $ insertMany "team" [ ["name" =: "Yankees", "league" =: "American"]
                                                  , ["name" =: "Dodgers", "league" =: "American"]
                                                  ]
      result <- db $ rest =<< find (select [] "team")
      result `shouldBe` [ ["_id" =: _id1, "name" =: "Yankees", "league" =: "American"]
                        , ["_id" =: _id2, "name" =: "Dodgers", "league" =: "American"]
                        ]
    context "Insert a document with duplicating key" $ do
      before (insertDuplicateWith insertMany `catch` \(_ :: Failure) -> return ()) $ do
        it "inserts documents before it" $
          db (count $ select ["name" =: "Yankees", "league" =: "American"] "team") `shouldReturn` 1

        it "doesn't insert documents after it" $
          db (count $ select ["name" =: "Indians", "league" =: "American"] "team") `shouldReturn` 0

      it "raises exception" $
        insertDuplicateWith insertMany `shouldThrow` anyException
      -- TODO No way to call getLastError?

  describe "insertMany_" $ do
    it "inserts documents to the collection and returns nothing" $ do
      ids <- db $ insertMany_ "team" [ ["name" =: "Yankees", "league" =: "American"]
                                     , ["name" =: "Dodgers", "league" =: "American"]
                                     ]
      ids `shouldBe` ()
    it "fails if the document is too big" $ do
      (db $ insertMany_ "hugeDocCollection" [hugeDocument]) `shouldThrow` anyException


    context "Insert a document with duplicating key" $ do
      before (insertDuplicateWith insertMany_ `catch` \(_ :: Failure) -> return ()) $ do
        it "inserts documents before it" $
          db (count $ select ["name" =: "Yankees", "league" =: "American"] "team") `shouldReturn` 1
        it "doesn't insert documents after it" $
          db (count $ select ["name" =: "Indians", "league" =: "American"] "team") `shouldReturn` 0
      it "raises exception" $
        insertDuplicateWith insertMany_ `shouldThrow` anyException

  describe "insertAll" $ do
    it "inserts documents to the collection and returns their _ids" $ do
      (_id1:_id2:_) <- db $ insertAll "team" [ ["name" =: "Yankees", "league" =: "American"]
                                             , ["name" =: "Dodgers", "league" =: "American"]
                                             ]
      result <- db $ rest =<< find (select [] "team")
      result `shouldBe` [["_id" =: _id1, "name" =: "Yankees", "league" =: "American"]
                        ,["_id" =: _id2, "name" =: "Dodgers", "league" =: "American"]
                        ]

    context "Insert a document with duplicating key" $ do
      before (insertDuplicateWith insertAll `catch` \(_ :: Failure) -> return ()) $ do
        it "inserts all documents which can be inserted" $ do
          db (count $ select ["name" =: "Yankees", "league" =: "American"] "team") `shouldReturn` 1
          db (count $ select ["name" =: "Indians", "league" =: "American"] "team") `shouldReturn` 1

      it "raises exception" $
        insertDuplicateWith insertAll `shouldThrow` anyException

  describe "insertAll_" $ do
    it "inserts documents to the collection and returns their _ids" $ do
      ids <- db $ insertAll_ "team" [ ["name" =: "Yankees", "league" =: "American"]
                                        , ["name" =: "Dodgers", "league" =: "American"]
                                        ]
      ids `shouldBe` ()

    context "Insert a document with duplicating key" $ do
      before (insertDuplicateWith insertAll_ `catch` \(_ :: Failure) -> return ()) $ do
        it "inserts all documents which can be inserted" $ do
          db (count $ select ["name" =: "Yankees", "league" =: "American"] "team") `shouldReturn` 1
          db (count $ select ["name" =: "Indians", "league" =: "American"] "team") `shouldReturn` 1

      it "raises exception" $
        insertDuplicateWith insertAll_ `shouldThrow` anyException

  describe "insertAll_" $ do
    it "inserts documents and receives 100 000 of them" $ do
      let docs = (flip map) [0..200000] $ \i ->
              ["name" =: (T.pack $ "name " ++ (show i))]
      db $ insertAll_ "bigCollection" docs
      db $ do
        cur <- find $ (select [] "bigCollection") {limit = 100000, batchSize = 100000}
        returnedDocs <- rest cur

        liftIO $ (length returnedDocs) `shouldBe` 100000

  describe "insertAll_" $ do
    it "inserts big documents" $ do
      let docs = replicate 100 bigDocument
      db $ insertAll_ "bigDocCollection" docs
      db $ do
        cur <- find $ (select [] "bigDocCollection") {limit = 100000, batchSize = 100000}
        returnedDocs <- rest cur

        liftIO $ (length returnedDocs) `shouldBe` 100
    it "inserts fine grained big documents" $ do
      let docs = replicate 1000 fineGrainedBigDocument
      db $ insertAll_ "bigDocFineGrainedCollection" docs
      db $ do
        cur <- find $ (select [] "bigDocFineGrainedCollection") {limit = 100000, batchSize = 100000}
        returnedDocs <- rest cur

        liftIO $ (length returnedDocs) `shouldBe` 1000
    it "skips one too big document" $ do
      (db $ insertAll_ "hugeDocCollection" [hugeDocument]) `shouldThrow` anyException
      db $ do
        cur <- find $ (select [] "hugeDocCollection") {limit = 100000, batchSize = 100000}
        returnedDocs <- rest cur

        liftIO $ (length returnedDocs) `shouldBe` 0

  describe "rest" $ do
    it "returns all documents from the collection" $ do
      let docs = (flip map) [0..6000] $ \i ->
              ["name" =: (T.pack $ "name " ++ (show i))]
          collectionName = "smallCollection"
      db $ insertAll_ collectionName docs
      db $ do
        cur <- find $ (select [] collectionName)
        returnedDocs <- rest cur

        liftIO $ (length returnedDocs) `shouldBe` 6001

  describe "updateMany" $ do
    it "updates value" $ do
      wireVersion <- getWireVersion
      when (wireVersion > 1) $ do
        _id <- db $ insert "team" ["name" =: "Yankees", "league" =: "American"]
        result <- db $ rest =<< find (select [] "team")
        result `shouldBe` [["_id" =: _id, "name" =: "Yankees", "league" =: "American"]]
        _ <- db $ updateMany "team" [([ "_id" =: _id]
                                      , ["$set" =: ["league" =: "European"]]
                                      , [])]
        updatedResult <- db $ rest =<< find (select [] "team")
        updatedResult `shouldBe` [["_id" =: _id, "name" =: "Yankees", "league" =: "European"]]
    it "upserts value" $ do
      wireVersion <- getWireVersion
      when (wireVersion > 1) $ do
        c <- db $ count (select [] "team")
        c `shouldBe` 0
        _ <- db $ updateMany "team" [( []
                                     , ["name" =: "Giants", "league" =: "MLB"]
                                     , [Upsert]
                                     )]
        updatedResult <- db $ rest =<< find ((select [] "team") {project = ["_id" =: (0 :: Int)]})
        map L.sort updatedResult `shouldBe` [["league" =: "MLB", "name" =: "Giants"]]
    it "updates all documents with Multi enabled" $ do
      wireVersion <- getWireVersion
      when (wireVersion > 1) $ do
        _ <- db $ insert "team" ["name" =: "Yankees", "league" =: "American"]
        _ <- db $ insert "team" ["name" =: "Yankees", "league" =: "MiLB"]
        _ <- db $ updateMany "team" [( ["name" =: "Yankees"]
                                     , ["$set" =: ["league" =: "MLB"]]
                                     , [MultiUpdate]
                                     )]
        updatedResult <- db $ rest =<< find ((select [] "team") {project = ["_id" =: (0 :: Int)]})
        (L.sort $ map L.sort updatedResult) `shouldBe` [ ["league" =: "MLB", "name" =: "Yankees"]
                                                       , ["league" =: "MLB", "name" =: "Yankees"]
                                                       ]
    it "updates one document when there is no Multi option" $ do
      wireVersion <- getWireVersion
      when (wireVersion > 1) $ do
        _ <- db $ insert "team" ["name" =: "Yankees", "league" =: "American"]
        _ <- db $ insert "team" ["name" =: "Yankees", "league" =: "MiLB"]
        _ <- db $ updateMany "team" [( ["name" =: "Yankees"]
                                     , ["$set" =: ["league" =: "MLB"]]
                                     , []
                                     )]
        updatedResult <- db $ rest =<< find ((select [] "team") {project = ["_id" =: (0 :: Int)]})
        (L.sort $ map L.sort updatedResult) `shouldBe` [ ["league" =: "MLB", "name" =: "Yankees"]
                                                       , ["league" =: "MiLB", "name" =: "Yankees"]
                                                       ]
    it "can process different updates" $ do
      wireVersion <- getWireVersion
      when (wireVersion > 1) $ do
        _ <- db $ insert "team" ["name" =: "Yankees", "league" =: "American"]
        _ <- db $ insert "team" ["name" =: "Giants" , "league" =: "MiLB"]
        _ <- db $ updateMany "team" [ ( ["name" =: "Yankees"]
                                      , ["$set" =: ["league" =: "MiLB"]]
                                      , []
                                      )
                                    , ( ["name" =: "Giants"]
                                      , ["$set" =: ["league" =: "MLB"]]
                                      , []
                                      )
                                    ]
        updatedResult <- db $ rest =<< find ((select [] "team") {project = ["_id" =: (0 :: Int)]})
        (L.sort $ map L.sort updatedResult) `shouldBe` [ ["league" =: "MLB" , "name" =: "Giants"]
                                                       , ["league" =: "MiLB", "name" =: "Yankees"]
                                                       ]
    it "can process different updates" $ do
      wireVersion <- getWireVersion
      when (wireVersion > 1) $ do
        _ <- db $ insert "team" ["name" =: "Yankees", "league" =: "American", "score" =: (Nothing :: Maybe Int)]
        _ <- db $ insert "team" ["name" =: "Giants" , "league" =: "MiLB", "score" =: (1 :: Int)]
        updateResult <- (db $ updateMany "team" [ ( ["name" =: "Yankees"]
                                                , ["$inc" =: ["score" =: (1 :: Int)]]
                                                , []
                                                )
                                              , ( ["name" =: "Giants"]
                                                , ["$inc" =: ["score" =: (2 :: Int)]]
                                                , []
                                                )
                                              ])
        failed updateResult `shouldBe` True
        updatedResult <- db $ rest =<< find ((select [] "team") {project = ["_id" =: (0 :: Int)]})
        (L.sort $ map L.sort updatedResult) `shouldBe` [ ["league" =: "American", "name" =: "Yankees", "score" =: (Nothing :: Maybe Int)]
                                                       , ["league" =: "MiLB"    , "name" =: "Giants" , "score" =: (1 :: Int)]
                                                       ]
    it "can handle big updates" $ do
      wireVersion <- getWireVersion
      when (wireVersion > 1) $ do
        let docs = (flip map) [0..20000] $ \i ->
                ["name" =: (T.pack $ "name " ++ (show i))]
        ids <- db $ insertAll "bigCollection" docs
        let updateDocs = (flip map) ids (\i -> ( [ "_id" =: i]
                                        , ["$set" =: ["name" =: ("name " ++ (show i))]]
                                        , []
                                        ))
        _ <- db $ updateMany "team" updateDocs
        updatedResult <- db $ rest =<< find (select [] "team")
        forM_ updatedResult $ \r -> let (i :: ObjectId) = "_id" `at` r
                                     in (("name" `at` r) :: String) `shouldBe` ("name" ++ (show i))

  describe "updateAll" $ do
    it "can process different updates" $ do
      wireVersion <- getWireVersion
      when (wireVersion > 1) $ do
        _ <- db $ insert "team" ["name" =: "Yankees", "league" =: "American", "score" =: (Nothing :: Maybe Int)]
        _ <- db $ insert "team" ["name" =: "Giants" , "league" =: "MiLB", "score" =: (1 :: Int)]
        updateResult <- (db $ updateAll "team" [ ( ["name" =: "Yankees"]
                                               , ["$inc" =: ["score" =: (1 :: Int)]]
                                               , []
                                               )
                                             , ( ["name" =: "Giants"]
                                               , ["$inc" =: ["score" =: (2 :: Int)]]
                                               , []
                                               )
                                             ])
        failed updateResult `shouldBe` True
        updatedResult <- db $ rest =<< find ((select [] "team") {project = ["_id" =: (0 :: Int)]})
        (L.sort $ map L.sort updatedResult) `shouldBe` [ ["league" =: "American", "name" =: "Yankees", "score" =: (Nothing :: Maybe Int)]
                                                       , ["league" =: "MiLB"    , "name" =: "Giants" , "score" =: (3 :: Int)]
                                                       ]
    it "returns correct number of matched and modified" $ do
      wireVersion <- getWireVersion
      when (wireVersion > 1) $ do
        _ <- db $ insertMany "testCollection" [["myField" =: "myValue"], ["myField2" =: "myValue2"]]
        _ <- db $ insertMany "testCollection" [["myField" =: "myValue"], ["myField2" =: "myValue2"]]
        res <- db $ updateMany "testCollection" [(["myField" =: "myValue"], ["$set" =: ["myField" =: "newValue"]], [MultiUpdate])]
        nMatched res `shouldBe` 2
        nModified res `shouldBe` (Just 2)
    it "returns correct number of upserted" $ do
      wireVersion <- getWireVersion
      when (wireVersion > 1) $ do
        res <- db $ updateMany "testCollection" [(["myField" =: "myValue"], ["$set" =: ["myfield" =: "newValue"]], [Upsert])]
        (length $ upserted res) `shouldBe` 1
    it "updates only one doc without multi update" $ do
      wireVersion <- getWireVersion
      when (wireVersion > 1) $ do
        _ <- db $ insertMany "testCollection" [["myField" =: "myValue"], ["myField2" =: "myValue2"]]
        _ <- db $ insertMany "testCollection" [["myField" =: "myValue"], ["myField2" =: "myValue2"]]
        res <- db $ updateMany "testCollection" [(["myField" =: "myValue"], ["$set" =: ["myField" =: "newValue"]], [])]
        nMatched res `shouldBe` 1
        nModified res `shouldBe` (Just 1)

  describe "delete" $ do
    it "actually deletes something" $ do
      _ <- db $ insert "team" ["name" =: ("Giants" :: String)]
      db $ delete $ select ["name" =: "Giants"] "team"
      updatedResult <- db $ rest =<< find ((select [] "team") {project = ["_id" =: (0 :: Int)]})
      length updatedResult `shouldBe` 0
    it "deletes all matching entries" $ do
      _ <- db $ insert "team" ["name" =: ("Giants" :: String)]
      _ <- db $ insert "team" [ "name"  =: ("Giants" :: String)
                              , "score" =: (10 :: Int)
                              ]
      db $ delete $ select ["name" =: "Giants"] "team"
      updatedResult <- db $ rest =<< find ((select [] "team") {project = ["_id" =: (0 :: Int)]})
      length updatedResult `shouldBe` 0
    it "works if there is no matching document" $ do
      db $ delete $ select ["name" =: "Giants"] "team"
      updatedResult <- db $ rest =<< find ((select [] "team") {project = ["_id" =: (0 :: Int)]})
      length updatedResult `shouldBe` 0

  describe "deleteOne" $ do
    it "actually deletes something" $ do
      _ <- db $ insert "team" ["name" =: ("Giants" :: String)]
      db $ deleteOne $ select ["name" =: "Giants"] "team"
      updatedResult <- db $ rest =<< find ((select [] "team") {project = ["_id" =: (0 :: Int)]})
      length updatedResult `shouldBe` 0
    it "deletes only one matching entry" $ do
      _ <- db $ insert "team" ["name" =: ("Giants" :: String)]
      _ <- db $ insert "team" [ "name"  =: ("Giants" :: String)
                              , "score" =: (10 :: Int)
                              ]
      db $ deleteOne $ select ["name" =: "Giants"] "team"
      updatedResult <- db $ rest =<< find ((select [] "team") {project = ["_id" =: (0 :: Int)]})
      length updatedResult `shouldBe` 1
    it "works if there is no matching document" $ do
      db $ deleteOne $ select ["name" =: "Giants"] "team"
      updatedResult <- db $ rest =<< find ((select [] "team") {project = ["_id" =: (0 :: Int)]})
      length updatedResult `shouldBe` 0

  describe "deleteMany" $ do
    it "actually deletes something" $ do
      wireVersion <- getWireVersion
      when (wireVersion > 1) $ do
        _ <- db $ insert "team" ["name" =: ("Giants" :: String)]
        _ <- db $ insert "team" ["name" =: ("Yankees" :: String)]
        _ <- db $ deleteMany "team" [ (["name" =: ("Giants" :: String)], [])
                                    , (["name" =: ("Yankees" :: String)], [])
                                    ]
        updatedResult <- db $ rest =<< find ((select [] "team") {project = ["_id" =: (0 :: Int)]})
        length updatedResult `shouldBe` 0

  describe "deleteAll" $ do
    it "actually deletes something" $ do
      wireVersion <- getWireVersion
      when (wireVersion > 1) $ do
        _ <- db $ insert "team" [ "name"  =: ("Giants" :: String)
                                , "score" =: (Nothing :: Maybe Int)
                                ]
        _ <- db $ insert "team" [ "name" =: ("Yankees" :: String)
                                , "score" =: (1 :: Int)
                                ]
        _ <- db $ deleteAll "team" [ (["name" =: ("Giants" :: String)], [])
                                   , (["name" =: ("Yankees" :: String)], [])
                                   ]
        updatedResult <- db $ rest =<< find ((select [] "team") {project = ["_id" =: (0 :: Int)]})
        length updatedResult `shouldBe` 0
    it "can handle big deletes" $ do
      wireVersion <- getWireVersion
      when (wireVersion > 1) $ do
        let docs = (flip map) [0..20000] $ \i ->
                ["name" =: (T.pack $ "name " ++ (show i))]
        _ <- db $ insertAll "bigCollection" docs
        _ <- db $ deleteAll "bigCollection" $ map (\d -> (d, [])) docs
        updatedResult <- db $ rest =<< find ((select [] "bigCollection") {project = ["_id" =: (0 :: Int)]})
        length updatedResult `shouldBe` 0
    it "returns correct result" $ do
      wireVersion <- getWireVersion
      when (wireVersion > 1) $ do
        _ <- db $ insert "testCollection" [ "myField" =: "myValue" ]
        _ <- db $ insert "testCollection" [ "myField" =: "myValue" ]
        res <- db $ deleteAll "testCollection" [ (["myField" =: "myValue"], []) ]
        nRemoved res `shouldBe` 2

  describe "allCollections" $ do
    it "returns all collections in a database" $ do
      _ <- db $ insert "team1" ["name" =: "Yankees", "league" =: "American"]
      _ <- db $ insert "team2" ["name" =: "Yankees", "league" =: "American"]
      _ <- db $ insert "team3" ["name" =: "Yankees", "league" =: "American"]
      collections <- db $ allCollections
      liftIO $ (L.sort collections) `shouldContain` ["team1", "team2", "team3"]

  describe "aggregate" $ before_ insertUsers $
    it "aggregates to normalize and sort documents" $ do
      result <- db $ aggregate "users" [ ["$project" =: ["name" =: ["$toUpper" =: "$_id"], "_id" =: 0]]
                                       , ["$sort" =: ["name" =: 1]]
                                       ]
      result `shouldBe` [["name" =: "JANE"], ["name" =: "JILL"], ["name" =: "JOE"]]

  -- This feature was introduced in MongoDB version 3.2
  -- https://docs.mongodb.com/manual/reference/command/find/
  describe "findCommand" $ pendingIfMongoVersion (< (3,2)) $
    context "when mongo version is 3.2 or superior" $ before insertUsers $ do
      it "fetches all the records" $ do
          result <- db $ rest =<< findCommand (select [] "users")
          length result `shouldBe` 3

      it "filters the records" $ do
        result <- db $ rest =<< findCommand (select ["_id" =: "joe"] "users")
        length result `shouldBe` 1

      it "projects the records" $ do
        result <- db $ rest =<< findCommand
          (select [] "users") { project = [ "_id" =: 1 ] }
        result `shouldBe` [["_id" =: "jane"], ["_id" =: "joe"], ["_id" =: "jill"]]

      it "sorts the records" $ do
        result <- db $ rest =<< findCommand
          (select [] "users") { project = [ "_id" =: 1 ]
                              , sort    = [ "_id" =: 1 ]
                              }
        result `shouldBe` [["_id" =: "jane"], ["_id" =: "jill"], ["_id" =: "joe"]]

