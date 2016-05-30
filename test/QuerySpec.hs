{-# LANGUAGE OverloadedStrings, ExtendedDefaultRules, ScopedTypeVariables #-}
{-# OPTIONS_GHC -fno-warn-type-defaults #-}

module QuerySpec (spec) where
import Data.String (IsString(..))
import TestImport
import Control.Exception
import qualified Data.List as L

import qualified Data.Text as T

testDBName :: Database
testDBName = "mongodb-haskell-test"

db :: Action IO a -> IO a
db action = do
    pipe <- connect (host "127.0.0.1")
    result <- access pipe master testDBName action
    close pipe
    return result

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
      db $ insertAll_ "hugeDocCollection" [hugeDocument]
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

  describe "allCollections" $ do
    it "returns all collections in a database" $ do
      _ <- db $ insert "team1" ["name" =: "Yankees", "league" =: "American"]
      _ <- db $ insert "team2" ["name" =: "Yankees", "league" =: "American"]
      _ <- db $ insert "team3" ["name" =: "Yankees", "league" =: "American"]
      collections <- db $ allCollections
      liftIO $ (L.sort collections) `shouldContain` ["team1", "team2", "team3"]

  describe "aggregate" $ do
    it "aggregates to normalize and sort documents" $ do
      db $ insertAll_ "users" [ ["_id" =: "jane", "joined" =: parseDate "2011-03-02", "likes" =: ["golf", "racquetball"]]
                              , ["_id" =: "joe", "joined" =: parseDate "2012-07-02", "likes" =: ["tennis", "golf", "swimming"]]
                              , ["_id" =: "jill", "joined" =: parseDate "2013-11-17", "likes" =: ["cricket", "golf"]]
                              ]
      result <- db $ aggregate "users" [ ["$project" =: ["name" =: ["$toUpper" =: "$_id"], "_id" =: 0]]
                                       , ["$sort" =: ["name" =: 1]]
                                       ]
      result `shouldBe` [["name" =: "JANE"], ["name" =: "JILL"], ["name" =: "JOE"]]
