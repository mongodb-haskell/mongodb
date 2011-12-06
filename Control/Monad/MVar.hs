{- | Lift MVar operations so you can do them within monads stacked on top of IO. Analogous to MonadIO -}

{-# LANGUAGE FlexibleContexts, TupleSections #-}

module Control.Monad.MVar (
	MVar,
	module Control.Monad.MVar,
	liftIO,
) where

import Control.Concurrent.MVar (MVar)
import qualified Control.Concurrent.MVar as IO
import Control.Monad.Error (MonadIO (liftIO))
import Control.Monad.Trans.Control (MonadBaseControl, liftBaseWith)
import Control.Exception.Lifted (mask, onException)

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

modifyMVar :: (MonadIO m, MonadBaseControl IO m) => MVar a -> (a -> m (a, b)) -> m b
modifyMVar m io =
  mask $ \restore -> do
    a      <- takeMVar m
    (a',b) <- restore (io a) `onException` putMVar m a
    putMVar m a'
    return b

addMVarFinalizer :: (MonadIO m, MonadBaseControl IO m) => MVar a -> m () -> m ()
addMVarFinalizer mv f = liftBaseWith $ \run ->
  IO.addMVarFinalizer mv (run f >> return ())

modifyMVar_ :: (MonadIO m, MonadBaseControl IO m) => MVar a -> (a -> m a) -> m ()
modifyMVar_ var act = modifyMVar var $ \a -> do
	a' <- act a
	return (a', ())

withMVar :: (MonadIO m, MonadBaseControl IO m) => MVar a -> (a -> m b) -> m b
withMVar var act = modifyMVar var $ \a -> do
	b <- act a
	return (a, b)
