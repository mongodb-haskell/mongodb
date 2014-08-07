module QueryTest (querySpec) where
import TestImport

fakeDB :: MonadIO m => Action m a -> m a
fakeDB = access (error "Pipe") (error "AccessMode") "fake"

querySpec :: Spec
querySpec =
  describe "useDb" $
    it "changes the db" $ do
      db1 <- fakeDB thisDatabase
      db1 `shouldBe` "fake"
      db2 <- fakeDB $ useDb "use" thisDatabase
      db2 `shouldBe` "use"
