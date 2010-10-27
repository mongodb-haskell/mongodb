{- | Lift MVar operations so you can do them within monads stacked on top of IO. Analogous to MonadIO -}

{-# LANGUAGE TupleSections #-}

module Control.Monad.MVar (
	MVar,
	module Control.Monad.MVar,
	liftIO
) where

import Control.Concurrent.MVar (MVar)
import qualified Control.Concurrent.MVar as IO
import Control.Monad.Error
import Control.Monad.Reader
import Control.Monad.State

newEmptyMVar :: (MonadIO m) => m (MVar a)
newEmptyMVar = liftIO IO.newEmptyMVar

newMVar :: (MonadIO m) => a -> m (MVar a)
newMVar = liftIO . IO.newMVar

takeMVar :: (MonadIO m) => MVar a -> m a
takeMVar = liftIO . IO.takeMVar

putMVar :: (MonadIO m) => MVar a -> a -> m ()
putMVar var = liftIO . IO.putMVar var

readMVar :: (MonadIO m) => MVar a -> m a
readMVar = liftIO . IO.readMVar

swapMVar :: (MonadIO m) => MVar a -> a -> m a
swapMVar var = liftIO . IO.swapMVar var

tryTakeMVar :: (MonadIO m) => MVar a -> m (Maybe a)
tryTakeMVar = liftIO . IO.tryTakeMVar

tryPutMVar :: (MonadIO m) => MVar a -> a -> m Bool
tryPutMVar var = liftIO . IO.tryPutMVar var

isEmptyMVar :: (MonadIO m) => MVar a -> m Bool
isEmptyMVar = liftIO . IO.isEmptyMVar

class (MonadIO m) => MonadMVar m where
	modifyMVar :: MVar a -> (a -> m (a, b)) -> m b
	addMVarFinalizer :: MVar a -> m () -> m ()

modifyMVar_ :: (MonadMVar m) => MVar a -> (a -> m a) -> m ()
modifyMVar_ var act = modifyMVar var $ \a -> do
	a' <- act a
	return (a', ())

withMVar :: (MonadMVar m) => MVar a -> (a -> m b) -> m b
withMVar var act = modifyMVar var $ \a -> do
	b <- act a
	return (a, b)

instance MonadMVar IO where
	modifyMVar = IO.modifyMVar
	addMVarFinalizer = IO.addMVarFinalizer

instance (MonadMVar m, Error e) => MonadMVar (ErrorT e m) where
	modifyMVar var f = ErrorT $ modifyMVar var $ \a -> do
		e <- runErrorT (f a)
		return $ either ((a, ) . Left) (fmap Right) e
	addMVarFinalizer var (ErrorT act) = ErrorT $ 
		addMVarFinalizer var (act >> return ()) >> return (Right ())
		-- NOTE, error is silently dropped

instance (MonadMVar m) => MonadMVar (ReaderT r m) where
	modifyMVar var f = ReaderT $ \r -> modifyMVar var $ \a -> runReaderT (f a) r
	addMVarFinalizer var (ReaderT act) = ReaderT (addMVarFinalizer var . act)

instance (MonadMVar m) => MonadMVar (StateT s m) where
	modifyMVar var f = StateT $ \s -> modifyMVar var $ \a -> do
		((a', b), s') <- runStateT (f a) s
		return (a', (b, s'))
	addMVarFinalizer var (StateT act) = StateT $ \s ->
		addMVarFinalizer var (act s >> return ()) >> return ((), s)
