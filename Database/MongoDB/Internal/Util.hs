-- | Miscellaneous general functions and Show, Eq, and Ord instances for PortID

{-# LANGUAGE FlexibleInstances, UndecidableInstances, StandaloneDeriving #-}
-- PortID instances
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Database.MongoDB.Internal.Util where

import Control.Applicative (Applicative(..), (<$>))
import Network (PortID(..))
import Data.UString as U (cons, append)
import Data.Bits (Bits, (.|.))
import Data.Bson
import Data.ByteString.Lazy as S (ByteString, length, append, hGet)
import System.IO (Handle)
import System.IO.Error (mkIOError, eofErrorType)
import Control.Exception (assert)
import Control.Monad.Error
import Control.Arrow (left)
import qualified Data.ByteString as BS (ByteString, unpack)
import Data.Word (Word8)
import Numeric (showHex)
import System.Random.Shuffle (shuffle')
import System.Random (newStdGen)
import Data.List as L (length)

deriving instance Show PortID
deriving instance Eq PortID
deriving instance Ord PortID

-- | MonadIO with extra Applicative and Functor superclasses
class (MonadIO m, Applicative m, Functor m) => MonadIO' m
instance (MonadIO m, Applicative m, Functor m) => MonadIO' m

-- | A monadic sort implementation derived from the non-monadic one in ghc's Prelude
mergesortM :: Monad m => (a -> a -> m Ordering) -> [a] -> m [a]
mergesortM cmp = mergesortM' cmp . map wrap

mergesortM' :: Monad m => (a -> a -> m Ordering) -> [[a]] -> m [a]
mergesortM' _  [] = return []
mergesortM' _  [xs] = return xs
mergesortM' cmp xss = mergesortM' cmp =<< (merge_pairsM cmp xss)

merge_pairsM :: Monad m => (a -> a -> m Ordering) -> [[a]] -> m [[a]]
merge_pairsM _   [] = return []
merge_pairsM _   [xs] = return [xs]
merge_pairsM cmp (xs:ys:xss) = liftM2 (:) (mergeM cmp xs ys) (merge_pairsM cmp xss)

mergeM :: Monad m => (a -> a -> m Ordering) -> [a] -> [a] -> m [a]
mergeM _   [] ys = return ys
mergeM _   xs [] = return xs
mergeM cmp (x:xs) (y:ys)
 = do
     c <- x `cmp` y
     case c of
        GT -> liftM (y:) (mergeM cmp (x:xs)   ys)
        _  -> liftM (x:) (mergeM cmp    xs (y:ys))

wrap :: a -> [a]
wrap x = [x]

shuffle :: [a] -> IO [a]
-- ^ Randomly shuffle items in list
shuffle list = shuffle' list (L.length list) <$> newStdGen

loop :: (Functor m, Monad m) => m (Maybe a) -> m [a]
-- ^ Repeatedy execute action, collecting results, until it returns Nothing
loop act = act >>= maybe (return []) (\a -> (a :) <$> loop act)

untilSuccess :: (MonadError e m, Error e) => (a -> m b) -> [a] -> m b
-- ^ Apply action to elements one at a time until one succeeds. Throw last error if all fail. Throw 'strMsg' error if list is empty.
untilSuccess = untilSuccess' (strMsg "empty untilSuccess")

untilSuccess' :: (MonadError e m) => e -> (a -> m b) -> [a] -> m b
-- ^ Apply action to elements one at a time until one succeeds. Throw last error if all fail. Throw given error if list is empty
untilSuccess' e _ [] = throwError e
untilSuccess' _ f (x : xs) = catchError (f x) (\e -> untilSuccess' e f xs)

whenJust :: (Monad m) => Maybe a -> (a -> m ()) -> m ()
whenJust mVal act = maybe (return ()) act mVal

liftIOE :: (MonadIO m) => (e -> e') -> ErrorT e IO a -> ErrorT e' m a
-- ^ lift IOE monad to ErrorT monad over some MonadIO m
liftIOE f = ErrorT . liftIO . fmap (left f) . runErrorT

runIOE :: ErrorT IOError IO a -> IO a
-- ^ Run action while catching explicit error and rethrowing in IO monad
runIOE (ErrorT action) = action >>= either ioError return

updateAssocs :: (Eq k) => k -> v -> [(k, v)] -> [(k, v)]
-- ^ Change or insert value of key in association list
updateAssocs key valu assocs = case back of [] -> (key, valu) : front; _ : back' -> front ++ (key, valu) : back'
	where (front, back) = break ((key ==) . fst) assocs

bitOr :: (Bits a) => [a] -> a
-- ^ bit-or all numbers together
bitOr = foldl (.|.) 0

(<.>) :: UString -> UString -> UString
-- ^ Concat first and second together with period in between. Eg. @\"hello\" \<.\> \"world\" = \"hello.world\"@
a <.> b = U.append a (cons '.' b)

true1 :: Label -> Document -> Bool
-- ^ Is field's value a 1 or True (MongoDB use both Int and Bools for truth values). Error if field not in document or field not a Num or Bool.
true1 k doc = case valueAt k doc of
	Bool b -> b
	Float n -> n == 1
	Int32 n -> n == 1
	Int64 n -> n == 1
	_ -> error $ "expected " ++ show k ++ " to be Num or Bool in " ++ show doc

hGetN :: Handle -> Int -> IO ByteString
-- ^ Read N bytes from hande, blocking until all N bytes are read. If EOF is reached before N bytes then raise EOF exception.
hGetN h n = assert (n >= 0) $ do
	bytes <- hGet h n
	let x = fromEnum $ S.length bytes
	if x >= n then return bytes
		else if x == 0 then ioError (mkIOError eofErrorType "hGetN" (Just h) Nothing)
			else S.append bytes <$> hGetN h (n - x)

byteStringHex :: BS.ByteString -> String
-- ^ Hexadecimal string representation of a byte string. Each byte yields two hexadecimal characters.
byteStringHex = concatMap byteHex . BS.unpack

byteHex :: Word8 -> String
-- ^ Two char hexadecimal representation of byte
byteHex b = (if b < 16 then ('0' :) else id) (showHex b "")
