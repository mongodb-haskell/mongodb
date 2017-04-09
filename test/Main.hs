module Main where

import Database.MongoDB.Admin (serverVersion)
import Database.MongoDB.Connection (connect, host)
import Database.MongoDB.Query (access, slaveOk)
import Data.Text (unpack)
import Test.Hspec.Runner
import System.Environment (getEnv)
import System.IO.Error (catchIOError)
import TestImport
import qualified Spec

main :: IO ()
main = do
  mongodbHost <- getEnv mongodbHostEnvVariable `catchIOError` (\_ -> return "localhost")
  p <- connect $ host mongodbHost
  version <- access p slaveOk "admin" serverVersion
  putStrLn $ "Running tests with mongodb version: " ++ (unpack version)
  hspecWith defaultConfig Spec.spec
