-- | Miscellaneous general functions

{-# LANGUAGE StandaloneDeriving #-}

module Database.MongoDB.Util where

import Prelude hiding (length)
import Network (PortID(..))
import Control.Applicative (Applicative(..), (<$>))
import Control.Exception (assert)
import Control.Monad.Reader
import Control.Monad.Error
import Data.UString as U (UString, cons, append)
import Data.Bits (Bits, (.|.))
import Data.Bson
import System.IO (Handle)
import Data.ByteString.Lazy as B (ByteString, length, append, hGet)

deriving instance Show PortID
deriving instance Eq PortID
deriving instance Ord PortID

instance (Monad m) => Applicative (ReaderT r m) where
	pure = return
	(<*>) = ap

instance (Monad m, Error e) => Applicative (ErrorT e m) where
	pure = return
	(<*>) = ap

ignore :: (Monad m) => a -> m ()
ignore _ = return ()

type Secs = Float

bitOr :: (Bits a) => [a] -> a
-- ^ bit-or all numbers together
bitOr = foldl (.|.) 0

(<.>) :: UString -> UString -> UString
-- ^ Concat first and second together with period in between. Eg. @\"hello\" \<.\> \"world\" = \"hello.world\"@
a <.> b = U.append a (cons '.' b)

loop :: (Functor m, Monad m) => m (Maybe a) -> m [a]
-- ^ Repeatedy execute action, collecting results, until it returns Nothing
loop act = act >>= maybe (return []) (\a -> (a :) <$> loop act)

true1 :: Label -> Document -> Bool
-- ^ Is field's value a 1 or True (MongoDB use both Int and Bools for truth values). Error if field not in document or field not a Num or Bool.
true1 k doc = case valueAt k doc of
	Bool b -> b
	Float n -> n == 1
	Int32 n -> n == 1
	Int64 n -> n == 1
	_ -> error $ "expected " ++ show k ++ " to be Num or Bool in " ++ show doc

hGetN :: Handle -> Int -> IO ByteString
-- ^ Read N bytes from hande, blocking until all N bytes are read.
-- Unlike hGet which only blocks if no bytes are available, otherwise it returns the X bytes immediately available where X <= N.
hGetN h n = assert (n >= 0) $ do
	bytes <- hGet h n
	let x = fromIntegral (length bytes)
	if x >= n then return bytes else do
		remainingBytes <- hGetN h (n - x)
		return (B.append bytes remainingBytes)
