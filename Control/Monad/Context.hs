{- | This is just like Control.Monad.Reader.Class except you can access the context of any Reader in the monad stack instead of just the top one as long as the context types are different. If two or more readers in the stack have the same context type you get the context of the top one. -}

{-# LANGUAGE MultiParamTypeClasses, FlexibleInstances, OverlappingInstances #-}

module Control.Monad.Context where

import Control.Monad.Reader
import Control.Monad.Error

-- | Same as 'MonadReader' but without functional dependency so the same monad can have multiple contexts with different types
class Context x m where
	context :: m x
	-- ^ Get the context in the Reader in the monad stack that has @x@ context type. Analogous to 'ask'.
	push :: (x -> x) -> m a -> m a 
	-- ^ Push new context in the Reader in the monad stack that has @x@ context type. Analogous to 'local'

instance (Monad m) => Context x (ReaderT x m) where
	context = ask
	push = local

instance (Monad m, Context x m) => Context x (ReaderT r m) where
	context = lift context
	push f m = ReaderT (push f . runReaderT m)

instance (Monad m, Context x m, Error e) => Context x (ErrorT e m) where
	context = lift context
	push f = ErrorT . push f . runErrorT
