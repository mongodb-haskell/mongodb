{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase #-}

module TestImport (
  module TestImport,
  module Export,
) where

import Control.Exception (SomeException (SomeException), try)
import Control.Monad.Trans as Export (MonadIO, liftIO)
import qualified Data.Text as T
import Data.Time (ParseTime, UTCTime)
import qualified Data.Time as Time
import Database.MongoDB as Export
import Test.Hspec as Export hiding (Selector)

-- We support the old version of time because it's easier than trying to use
-- only the new version and test older GHC versions.
#if MIN_VERSION_time(1,5,0)
import Data.Time.Format (defaultTimeLocale, iso8601DateFormat)
#else
import System.Locale (defaultTimeLocale, iso8601DateFormat)
import Data.Maybe (fromJust)
#endif

parseTime :: (ParseTime t) => String -> String -> t
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

data MongoAtlas = MongoAtlas
  { atlas_host :: T.Text
  , atlas_user :: T.Text
  , atlas_password :: T.Text
  }

extractMongoAtlasCredentials :: T.Text -> MongoAtlas
extractMongoAtlasCredentials cs =
  let s = T.drop 14 cs
      [u, s'] = T.splitOn ":" s
      [p, h] = T.splitOn "@" s'
   in MongoAtlas h u p

connectAtlas :: MongoAtlas -> IO Pipe
connectAtlas (MongoAtlas h _ _) = do
  repset <- openReplicaSetSRV' $ T.unpack h
  primaryOrSecondary repset >>= \case
    Just pipe -> pure pipe
    Nothing -> ioError $ error "Unable to acquire pipe from MongoDB Atlas' replicaset"
 where
  primaryOrSecondary rep =
    try (primary rep) >>= \case
      Left (SomeException err) -> do
        print $
          "Failed to acquire primary replica, reason:"
            ++ show err
            ++ ". Moving to second..."
        try (secondaryOk rep) >>= \case
          Left (SomeException _) -> pure Nothing
          Right pipe -> pure $ Just pipe
      Right pipe -> pure $ Just pipe
