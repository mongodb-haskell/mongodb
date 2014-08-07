module Main where
import Test.Hspec (hspec)

import QueryTest

main :: IO ()    
main = hspec $ do
  querySpec
