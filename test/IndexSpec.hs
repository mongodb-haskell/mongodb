{-# LANGUAGE OverloadedStrings, ExtendedDefaultRules, ScopedTypeVariables #-}
{-# OPTIONS_GHC -fno-warn-type-defaults #-}

module IndexSpec (spec) where

import TestImport
import System.Environment (getEnv)
import System.IO.Error (catchIOError)

testDBName :: Database
testDBName = "mongodb-haskell-test"

db :: Action IO a -> IO a
db action = do
    mongodbHost <- getEnv mongodbHostEnvVariable `catchIOError` (\_ -> return "localhost")
    pipe <- connect (host mongodbHost)
    result <- access pipe master testDBName action
    close pipe
    return result

withCleanDatabase :: ActionWith () -> IO ()
withCleanDatabase action = dropDB >> action () >> dropDB >> return ()
  where
    dropDB = db $ dropDatabase testDBName

spec :: Spec
spec = around withCleanDatabase $ do
  describe "make index" $ do
    it "single row index" $ do
      db (createIndex (index "thiscollection" ["field" =: (1::Int)]) >> getIndexes "thiscollection") `shouldReturn` [["field" =: (1::Int)]]
