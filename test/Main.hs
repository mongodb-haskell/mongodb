module Main where

import Control.Exception (assert)
import Control.Monad (when)
import Data.Maybe (isJust)
import qualified Spec
import System.Environment (getEnv, lookupEnv)
import Test.Hspec.Runner
import TestImport

main :: IO ()
main = do
  version <- getEnv "MONGO_VERSION"
  when (version == "mongo_atlas") $ do
    connection_string <- lookupEnv "CONNECTION_STRING"
    pure $ assert (isJust connection_string) ()
  hspecWith defaultConfig Spec.spec
