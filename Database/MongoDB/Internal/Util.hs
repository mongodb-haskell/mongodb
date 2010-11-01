-- | Miscellaneous general functions and Show, Eq, and Ord instances for PortID

{-# LANGUAGE StandaloneDeriving #-}

module Database.MongoDB.Internal.Util where

import Prelude hiding (length)
import Network (PortID(..))
import Data.UString as U (cons, append)
import Data.Bits (Bits, (.|.))
import Data.Bson

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
