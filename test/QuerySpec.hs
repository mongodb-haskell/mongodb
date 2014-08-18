{-# LANGUAGE OverloadedStrings, ExtendedDefaultRules, ScopedTypeVariables #-}
{-# OPTIONS_GHC -fno-warn-type-defaults #-}

module QuerySpec (spec) where
import TestImport
import Control.Exception

testDBName :: Database
testDBName = "mongodb-haskell-test"

fakeDB :: Action IO a -> IO a
fakeDB action = do
    pipe <- connect (host "127.0.0.1")
    result <- access pipe master testDBName action
    close pipe
    return result

withCleanDatabase :: IO a -> IO ()
withCleanDatabase action = dropDB >> action >> dropDB >> return ()
  where
    dropDB = fakeDB $ dropDatabase testDBName

spec :: Spec
spec = around withCleanDatabase $ do
  describe "useDb" $ do
    it "changes the db" $ do
      db1 <- fakeDB thisDatabase
      db1 `shouldBe` testDBName
      db2 <- fakeDB $ useDb "another-mongodb-haskell-test" thisDatabase
      db2 `shouldBe` "another-mongodb-haskell-test"

  describe "insert" $ do
    it "inserts a document to the collection and returns its _id" $ do
      _id <- fakeDB $ insert "team" ["name" =: "Yankees", "league" =: "American"]
      result <- fakeDB $ rest =<< find (select [] "team")
      result `shouldBe` [["_id" =: _id, "name" =: "Yankees", "league" =: "American"]]

  describe "insert_" $ do
    it "inserts a document to the collection and doesn't return _id" $ do
      _id <- fakeDB $ insert_ "team" ["name" =: "Yankees", "league" =: "American"]
      fakeDB (count $ select ["name" =: "Yankees", "league" =: "American"] "team") `shouldReturn` 1
      _id `shouldBe` ()

  describe "insertMany" $ do
    it "inserts documents to the collection and returns their _ids" $ do
      (_id1:_id2:_) <- fakeDB $ insertMany "team" [ ["name" =: "Yankees", "league" =: "American"]
                                                  , ["name" =: "Dodgers", "league" =: "American"]
                                                  ]
      result <- fakeDB $ rest =<< find (select [] "team")
      result `shouldBe` [ ["_id" =: _id1, "name" =: "Yankees", "league" =: "American"]
                        , ["_id" =: _id2, "name" =: "Dodgers", "league" =: "American"]
                        ]
    context "Insert a document with duplicating key" $ do
      let insertDuplicatingDocument = do
            _id <- fakeDB $ insert "team" ["name" =: "Dodgers", "league" =: "American"]
            _ <- fakeDB $ insertMany "team" [ ["name" =: "Yankees", "league" =: "American"]
                                            -- Try to insert document with
                                            -- duplicate key
                                            , ["name" =: "Dodgers", "league" =: "American", "_id" =: _id]
                                            , ["name" =: "Indians", "league" =: "American"]
                                            ]
            return ()

      before (insertDuplicatingDocument `catch` \(_ :: Failure) -> return ()) $ do
        it "inserts documents before it" $
          fakeDB ( count $ select ["name" =: "Yankees", "league" =: "American"] "team") `shouldReturn` 1

        it "doesn't insert documents after it" $
          fakeDB ( count $ select ["name" =: "Indians", "league" =: "American"] "team") `shouldReturn` 0

      it "raises exception" $
        insertDuplicatingDocument `shouldThrow` anyException
      -- TODO No way to call getLastError?

  describe "insertMany_" $ do
    it "inserts documents to the collection and returns nothing" $ do
      ids <- fakeDB $ insertMany_ "team" [ ["name" =: "Yankees", "league" =: "American"]
                                         , ["name" =: "Dodgers", "league" =: "American"]
                                         ]
      ids `shouldBe` ()

    context "Insert a document with duplicating key" $ do
      let insertDuplicatingDocument = do
            _id <- fakeDB $ insert "team" ["name" =: "Dodgers", "league" =: "American"]
            _ <- fakeDB $ insertMany_ "team" [ ["name" =: "Yankees", "league" =: "American"]
                                             -- Try to insert document with
                                             -- duplicate key
                                             , ["name" =: "Dodgers", "league" =: "American", "_id" =: _id]
                                             , ["name" =: "Indians", "league" =: "American"]
                                             ]
            return ()

      before (insertDuplicatingDocument `catch` \(_ :: Failure) -> return ()) $ do
        it "inserts documents before it" $
          fakeDB (count $ select ["name" =: "Yankees", "league" =: "American"] "team") `shouldReturn` 1
        it "doesn't insert documents after it" $
          fakeDB (count $ select ["name" =: "Indians", "league" =: "American"] "team") `shouldReturn` 0
      it "raises exception" $
        insertDuplicatingDocument `shouldThrow` anyException

  describe "insertAll" $ do
    it "inserts documents to the collection and returns their _ids" $ do
      (_id1:_id2:_) <- fakeDB $ insertAll "team" [ ["name" =: "Yankees", "league" =: "American"]
                                                 , ["name" =: "Dodgers", "league" =: "American"]
                                                 ]
      result <- fakeDB $ rest =<< find (select [] "team")
      result `shouldBe` [["_id" =: _id1, "name" =: "Yankees", "league" =: "American"]
                        ,["_id" =: _id2, "name" =: "Dodgers", "league" =: "American"]
                        ]

    context "Insert a document with duplicating key" $ do
      let insertDuplicatingDocument = do
            _id <- fakeDB $ insert "team" ["name" =: "Dodgers", "league" =: "American"]
            _ <- fakeDB $ insertAll "team" [ ["name" =: "Yankees", "league" =: "American"]
                                            -- Try to insert document with
                                            -- duplicate key
                                            , ["name" =: "Dodgers", "league" =: "American", "_id" =: _id]
                                            , ["name" =: "Indians", "league" =: "American"]
                                            ]
            return ()

      before (insertDuplicatingDocument `catch` \(_ :: Failure) -> return ()) $ do
        it "inserts all documents which can be inserted" $ do
          fakeDB (count $ select ["name" =: "Yankees", "league" =: "American"] "team") `shouldReturn` 1
          fakeDB (count $ select ["name" =: "Indians", "league" =: "American"] "team") `shouldReturn` 1

      it "raises exception" $
        insertDuplicatingDocument `shouldThrow` anyException

  describe "insertAll_" $ do
    it "inserts documents to the collection and returns their _ids" $ do
      ids <- fakeDB $ insertAll_ "team" [ ["name" =: "Yankees", "league" =: "American"]
                                        , ["name" =: "Dodgers", "league" =: "American"]
                                        ]
      ids `shouldBe` ()

    context "Insert a document with duplicating key" $ do
      let insertDuplicatingDocument = do
            _id <- fakeDB $ insert "team" ["name" =: "Dodgers", "league" =: "American"]
            _ <- fakeDB $ insertAll_ "team" [ ["name" =: "Yankees", "league" =: "American"]
                                             -- Try to insert document with
                                             -- duplicate key
                                             , ["name" =: "Dodgers", "league" =: "American", "_id" =: _id]
                                             , ["name" =: "Indians", "league" =: "American"]
                                             ]
            return ()

      before (insertDuplicatingDocument `catch` \(_ :: Failure) -> return ()) $ do
        it "inserts all documents which can be inserted" $ do
          fakeDB (count $ select ["name" =: "Yankees", "league" =: "American"] "team") `shouldReturn` 1
          fakeDB (count $ select ["name" =: "Indians", "league" =: "American"] "team") `shouldReturn` 1

      it "raises exception" $
        insertDuplicatingDocument `shouldThrow` anyException
