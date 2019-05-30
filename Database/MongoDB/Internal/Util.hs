-- | Miscellaneous general functions

{-# LANGUAGE FlexibleInstances, UndecidableInstances, StandaloneDeriving #-}
{-# LANGUAGE CPP #-}

module Database.MongoDB.Internal.Util where

#if !MIN_VERSION_base(4,8,0)
import Control.Applicative ((<$>))
#endif
import Control.Exception (handle, throwIO, Exception)
import Control.Monad (liftM, liftM2)
import Data.Bits (Bits, (.|.))
import Data.Word (Word8)
import Numeric (showHex)
import System.Random (newStdGen)
import System.Random.Shuffle (shuffle')

import qualified Data.ByteString as S

import Control.Monad.Except (MonadError(..))
import Control.Monad.Trans (MonadIO, liftIO)
import Data.Bson
import Data.Text (Text)

import qualified Data.Text as T

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
shuffle list = shuffle' list (length list) <$> newStdGen

loop :: Monad m => m (Maybe a) -> m [a]
-- ^ Repeatedy execute action, collecting results, until it returns Nothing
loop act = act >>= maybe (return []) (\a -> (a :) `liftM` loop act)

untilSuccess :: (MonadError e m) => (a -> m b) -> [a] -> m b
-- ^ Apply action to elements one at a time until one succeeds. Throw last error if all fail. Throw 'strMsg' error if list is empty.
untilSuccess = untilSuccess' (error "empty untilSuccess")
-- Use 'error' copying behavior in removed 'Control.Monad.Error.Error' instance:
-- instance Error Failure where strMsg = error
-- 'fail' is treated the same as a programming 'error'. In other words, don't use it.

untilSuccess' :: (MonadError e m) => e -> (a -> m b) -> [a] -> m b
-- ^ Apply action to elements one at a time until one succeeds. Throw last error if all fail. Throw given error if list is empty
untilSuccess' e _ [] = throwError e
untilSuccess' _ f (x : xs) = catchError (f x) (\e -> untilSuccess' e f xs)

whenJust :: (Monad m) => Maybe a -> (a -> m ()) -> m ()
whenJust mVal act = maybe (return ()) act mVal

liftIOE :: (MonadIO m, Exception e, Exception e') => (e -> e') -> IO a -> m a
-- ^ lift IOE monad to ErrorT monad over some MonadIO m
liftIOE f = liftIO . handle (throwIO . f)

updateAssocs :: (Eq k) => k -> v -> [(k, v)] -> [(k, v)]
-- ^ Change or insert value of key in association list
updateAssocs key valu assocs = case back of [] -> (key, valu) : front; _ : back' -> front ++ (key, valu) : back'
    where (front, back) = break ((key ==) . fst) assocs

bitOr :: (Num a, Bits a) => [a] -> a
-- ^ bit-or all numbers together
bitOr = foldl (.|.) 0

(<.>) :: Text -> Text -> Text
-- ^ Concat first and second together with period in between. Eg. @\"hello\" \<.\> \"world\" = \"hello.world\"@
a <.> b = T.append a (T.cons '.' b)

true1 :: Label -> Document -> Bool
-- ^ Is field's value a 1 or True (MongoDB use both Int and Bools for truth values). Error if field not in document or field not a Num or Bool.
true1 k doc = case valueAt k doc of
    Bool b -> b
    Float n -> n == 1
    Int32 n -> n == 1
    Int64 n -> n == 1
    _ -> error $ "expected " ++ show k ++ " to be Num or Bool in " ++ show doc

byteStringHex :: S.ByteString -> String
-- ^ Hexadecimal string representation of a byte string. Each byte yields two hexadecimal characters.
byteStringHex = concatMap byteHex . S.unpack

byteHex :: Word8 -> String
-- ^ Two char hexadecimal representation of byte
byteHex b = (if b < 16 then ('0' :) else id) (showHex b "")
