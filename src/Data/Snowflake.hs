{-# LANGUAGE BangPatterns #-}

module Data.Snowflake 
( SnowflakeConfig(..)
, Snowflake
, SnowflakeGen
, newSnowflakeGen
, nextSnowflake
, defaultConfig
) where

import Data.Bits  ((.|.), (.&.), shift, Bits)
import Control.Concurrent.MVar (MVar, newMVar, putMVar, takeMVar)
import Data.Time.Clock.POSIX (getPOSIXTime)
import Control.Monad (when)
import Control.Concurrent (threadDelay)

data SnowflakeConfig = SnowflakeConfig { confTimeBits  :: {-# UNPACK #-} !Int
                                       , confCountBits :: {-# UNPACK #-} !Int
                                       , confNodeBits  :: {-# UNPACK #-} !Int
                                       } deriving (Eq, Show)

defaultConfig :: SnowflakeConfig
defaultConfig = SnowflakeConfig 40 16 8

newtype SnowflakeGen = SnowflakeGen { genLastSnowflake :: MVar Snowflake } 

data Snowflake = Snowflake { snowflakeTime  :: !Integer
                           , snowflakeCount :: !Integer
                           , snowflakeNode  :: !Integer
                           , snowflakeConf  :: !SnowflakeConfig
                           } deriving (Eq)

snowflakeToInteger (Snowflake time count node config) = let
  SnowflakeConfig _ countBits nodeBits = config
  in 
    (time `shift` (countBits + nodeBits))
    .|.
    (count `shift` nodeBits)
    .|.
    node

instance Show Snowflake where
  show = show . snowflakeToInteger

cutBits :: (Num a, Bits a) => a -> Int -> a
cutBits n bits = n .&. ((1 `shift` bits) - 1)

currentTimestamp :: IO Integer
currentTimestamp = (round . (*1000)) `fmap` getPOSIXTime 

currentTimestampFixed :: Int -> IO Integer
currentTimestampFixed n = fmap (`cutBits` n) currentTimestamp

newSnowflakeGen ::  SnowflakeConfig -> Integer -> IO SnowflakeGen
newSnowflakeGen conf@(SnowflakeConfig timeBits _ nodeBits) nodeIdRaw = do
  timestamp <- currentTimestampFixed timeBits
  let nodeId    = nodeIdRaw `cutBits` nodeBits 
      initial    = Snowflake timestamp 0 nodeId conf
  mvar <- newMVar initial
  return $ SnowflakeGen mvar


nextSnowflake :: SnowflakeGen -> IO Snowflake
nextSnowflake (SnowflakeGen lastRef) = do
  Snowflake lastTime lastCount node conf <- takeMVar lastRef
  let SnowflakeConfig timeBits countBits _ = conf
      getNextTime = do
        time <- currentTimestampFixed timeBits
        if (lastTime > time) then do
          threadDelay $ fromInteger $ (lastTime - time) * 1000
          getNextTime
        else 
          return time
      loop = do
        timestamp <- getNextTime
        let count = if timestamp == lastTime then lastCount + 1 else 0
        if ((count `shift` (-1 * countBits)) /= 0) then 
          loop
        else 
          return $ Snowflake timestamp count node conf
  new <- loop          
  putMVar lastRef new
  return new
  
