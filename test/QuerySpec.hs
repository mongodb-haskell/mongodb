{-# LANGUAGE OverloadedStrings, ExtendedDefaultRules, ScopedTypeVariables #-}
{-# OPTIONS_GHC -fno-warn-type-defaults #-}

module QuerySpec (spec) where
import TestImport
import Control.Exception

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
