{-# LANGUAGE CPP #-}

module TestImport (
  module TestImport,
  module Export
) where

import Test.Hspec as Export hiding (Selector)
import Database.MongoDB as Export
import Control.Monad.Trans as Export (MonadIO, liftIO) 
import Data.Time (ParseTime, UTCTime)
import qualified Data.Time as Time

-- We support the old version of time because it's easier than trying to use
-- only the new version and test older GHC versions.
#if MIN_VERSION_time(1,5,0)
import Data.Time.Format (defaultTimeLocale, iso8601DateFormat)
#else
import System.Locale (defaultTimeLocale, iso8601DateFormat)
#endif

parseTime :: ParseTime t => String -> String -> t
#if MIN_VERSION_time(1,5,0)
parseTime = Time.parseTimeOrError True defaultTimeLocale
#else
parseTime fmt = fromJust . Time.parseTime defaultTimeLocale fmt
#endif

parseDate :: String -> UTCTime
parseDate = parseTime (iso8601DateFormat Nothing)

parseDateTime :: String -> UTCTime
parseDateTime = parseTime (iso8601DateFormat (Just "%H:%M:%S"))

mongodbHostEnvVariable :: String
mongodbHostEnvVariable = "HASKELL_MONGODB_TEST_HOST"
