# snowflake - haskell

A loose port of [Twitter's Snowflake](https://github.com/twitter/snowflake) to haskell. 

Generates arbitrary fixed-precision, unique and time-sortable identifiers.

## Usage
Generation lives inside IO Monad. 
```haskell
import Control.Monad
import Data.Snowflake

main = do
    gen <- newSnowflakeGen defaultConfig 0
    let next = nextSnowflake gen
    forM_ [1..20] $ const $ next >>= print
```

`nextSnowflake` is thread safe.
