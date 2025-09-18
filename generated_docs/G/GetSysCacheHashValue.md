# GetSysCacheHashValue

## Location
src/backend/utils/cache/syscache.c: 662 - 678

## Overview
Computes the hash value that would be used for a tuple in a specified system catalog cache with the given search keys.

## Definition


## Detailed Description
GetSysCacheHashValue calculates the hash value for a hypothetical tuple in a system catalog cache without actually performing a cache lookup. This function is primarily used for cache invalidation operations where external code needs to compute hash values to match against cached entries. The function validates the cache ID and delegates the actual hash computation to the underlying catalog cache system.

## Parameters / Member Variables
- `cacheId`: Integer identifier of the system cache to use for hash computation
- `key1`: First search key value (Datum type)
- `key2`: Second search key value (Datum type) 
- `key3`: Third search key value (Datum type)
- `key4`: Fourth search key value (Datum type)

## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid
  - GetCatCacheHashValue
- Called from (representative examples):
  - GetSysCacheHashValue1 (convenience macro)
  - GetSysCacheHashValue2 (convenience macro)
  - GetSysCacheHashValue3 (convenience macro)
  - GetSysCacheHashValue4 (convenience macro)

## Notes and Other Information
- The function performs bounds checking on cacheId and validates that the cache exists
- Hash values are exposed in cache invalidation operations, making this function necessary for external cache management
- The function is a thin wrapper around GetCatCacheHashValue, providing the syscache interface layer
- Located in src/backend/utils/cache/syscache.c:662-678