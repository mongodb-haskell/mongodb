
module Internal.ConnectionTest (
  spec,
) where

import Prelude hiding (read)
import Data.Monoid
import Data.IORef
import Control.Monad
import System.IO.Error (isEOFError)
import Test.Hspec

import Database.MongoDB.Internal.Connection

spec :: Spec
spec = describe "Internal.Connection" $ do
  readExactlySpec

readExactlySpec :: Spec
readExactlySpec = describe "readExactly" $ do
  it "should return specified number of bytes" $ do
    let conn = Connection
          { read = return "12345"
          , unread = \_ -> return ()
          , write = \_ -> return ()
          , flush = return ()
          , close = return ()
          }

    res <- readExactly conn 3
    res `shouldBe` "123"

  it "should unread the rest" $ do
    restRef <- newIORef mempty
    let conn = Connection
          { read = return "12345"
          , unread = writeIORef restRef
          , write = \_ -> return ()
          , flush = return ()
          , close = return ()
          }

    void $ readExactly conn 3
    rest <- readIORef restRef
    rest `shouldBe` "45"

  it "should ask for more bytes if the first chunk is too small" $ do
    let conn = Connection
          { read = return "12345"
          , unread = \_ -> return ()
          , write = \_ -> return ()
          , flush = return ()
          , close = return ()
          }

    res <- readExactly conn 8
    res `shouldBe` "12345123"

  it "should throw on EOF" $ do
    let conn = Connection
          { read = return mempty
          , unread = \_ -> return ()
          , write = \_ -> return ()
          , flush = return ()
          , close = return ()
          }

    void $ readExactly conn 3
      `shouldThrow` isEOFError
