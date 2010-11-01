-- | Extra monad functions and instances

{-# LANGUAGE FlexibleInstances, UndecidableInstances #-}

module Control.Monad.Util where

import Control.Applicative (Applicative(..), (<$>))
import Control.Arrow ((+++))
import Control.Monad.Reader
import Control.Monad.Error

-- | MonadIO with extra Applicative and Functor superclasses
class (MonadIO m, Applicative m, Functor m) => MonadIO' m
instance (MonadIO m, Applicative m, Functor m) => MonadIO' m

loop :: (Functor m, Monad m) => m (Maybe a) -> m [a]
-- ^ Repeatedy execute action, collecting results, until it returns Nothing
loop act = act >>= maybe (return []) (\a -> (a :) <$> loop act)

untilJust :: (Monad m) => (a -> m (Maybe b)) -> [a] -> m (Maybe b)
-- ^ Apply action to elements one at a time until one returns Just. Return Nothing if all return Nothing.
untilJust _ [] = return Nothing
untilJust f (a:as) = f a >>= maybe (untilJust f as) (return . Just)

untilSuccess :: (MonadError e m, Error e) => (a -> m b) -> [a] -> m b
-- ^ Apply action to elements one at a time until one succeeds. Throw last error if all fail. Throw 'strMsg' error if list is empty.
untilSuccess = untilSuccess' (strMsg "empty untilSuccess")

untilSuccess' :: (MonadError e m) => e -> (a -> m b) -> [a] -> m b
-- ^ Apply action to elements one at a time until one succeeds. Throw last error if all fail. Throw given error if list is empty
untilSuccess' e _ [] = throwError e
untilSuccess' _ f (x : xs) = catchError (f x) (\e -> untilSuccess' e f xs)

mapError :: (Functor m) => (e' -> e) -> ErrorT e' m a -> ErrorT e m a
-- ^ Convert error type thrown
mapError f (ErrorT m) = ErrorT $ (f +++ id) <$> m
