{- | Cycle through a set of resources (randomly), recreating them when they expire -}

{-# LANGUAGE RecordWildCards, NamedFieldPuns, FlexibleContexts #-}

module System.IO.Pool where

import Control.Applicative ((<$>))
import Control.Concurrent.MVar.Lifted
import Data.Array.IO
import Data.Maybe (catMaybes)
import Control.Monad.Error
import System.Random (randomRIO)
import Control.Exception (assert)

-- | Creator, destroyer, and checker of resources of type r. Creator may throw error or type e.
data Factory e r = Factory {
	newResource :: ErrorT e IO r,
	killResource :: r -> IO (),
	isExpired :: r -> IO Bool }

newPool :: Factory e r -> Int -> IO (Pool e r)
-- ^ Create new pool of initial max size, which must be >= 1
newPool f n = assert (n > 0) $ do
	arr <- newArray (0, n-1) Nothing
	var <- newMVar arr
	return (Pool f var)

data Pool e r = Pool {factory :: Factory e r, resources :: MVar (IOArray Int (Maybe r))}
-- ^ Pool of maximum N resources. Resources may expire on their own or be killed. Resources will initially be created on demand up N resources then recycled in random fashion. N may be changed by resizing the pool. Random is preferred to round-robin to distribute effect of pathological use cases that use every Xth resource the most and N is a multiple of X.
-- Resources *must* close/kill themselves when garbage collected ('resize' relies on this).

aResource :: (Error e) => Pool e r -> ErrorT e IO r
-- ^ Return a random live resource in pool or create new one if expired or not yet created
aResource Pool{..} = withMVar resources $ \array -> do
	i <- liftIO $ randomRIO =<< getBounds array
	mr <- liftIO $ readArray array i
	r <- maybe (new array i) (check array i) mr
	return r
 where
	new array i = do
		r <- newResource factory
		liftIO $ writeArray array i (Just r)
		return r
	check array i r = do
		bad <- liftIO $ isExpired factory r
		if bad then new array i else return r

poolSize :: Pool e r -> IO Int
-- ^ current max size of pool
poolSize Pool{resources} = withMVar resources (fmap rangeSize . getBounds)

resize :: Pool e r -> Int -> IO ()
-- ^ resize max size of pool. When shrinking some resource will be dropped without closing since they may still be in use. They are expected to close themselves when garbage collected.
resize Pool{resources} n = modifyMVar_ resources $ \array -> do
	rs <- take n <$> getElems array
	array' <- newListArray (0, n-1) (rs ++ repeat Nothing)
	return array'

killAll :: Pool e r -> IO ()
-- ^ Kill all resources in pool so subsequent access creates new ones
killAll (Pool Factory{killResource} resources) = withMVar resources $ \array -> do
	mapM_ killResource . catMaybes =<< getElems array
	mapM_ (\i -> writeArray array i Nothing) . range =<< getBounds array
