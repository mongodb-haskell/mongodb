module Main where
import Test.Hspec (hspec)

import QueryTest
import qualified Internal.ConnectionTest

main :: IO ()    
main = hspec $ do
  querySpec
  Internal.ConnectionTest.spec
