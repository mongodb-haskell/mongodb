-- | Miscellaneous general functions and Show, Eq, and Ord instances for PortID

{-# LANGUAGE StandaloneDeriving #-}

module Database.MongoDB.Internal.Util where

import Prelude hiding (length)
import Control.Applicative ((<$>))
import Network (PortID(..))
import Data.UString as U (cons, append)
import Data.Bits (Bits, (.|.))
import Data.Bson
import Data.ByteString.Lazy as S (ByteString, length, append, hGet)
import System.IO (Handle)
import System.IO.Error (mkIOError, eofErrorType)
import Control.Exception (assert)

deriving instance Show PortID
deriving instance Eq PortID
deriving instance Ord PortID

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
	let x = fromEnum $ length bytes
	if x >= n then return bytes
		else if x == 0 then ioError (mkIOError eofErrorType "hGetN" (Just h) Nothing)
			else S.append bytes <$> hGetN h (n - x)
