# CatalogCacheComputeTupleHashValue

## Location
src/backend/utils/cache/catcache.c: 386 - 440

## Overview
A static function that extracts key attribute values from a HeapTuple and computes the corresponding hash value for catalog cache storage and lookup operations.

## Definition


## Detailed Description
The `CatalogCacheComputeTupleHashValue` function serves as a tuple-oriented wrapper around `CatalogCacheComputeHashValue`. It extracts key values from a HeapTuple using the cache's configured key column numbers and tuple descriptor, then delegates to `CatalogCacheComputeHashValue` for the actual hash computation. The function uses `fastgetattr` to efficiently extract attribute values from the tuple and includes assertions to ensure that key attributes are never NULL (which would be a system catalog integrity violation). This function is essential for operations that need to compute hash values for existing tuples, such as cache invalidation and list-based searches where the full tuple is available but individual key values need to be extracted.

## Parameters / Member Variables
- `cache`: Pointer to the CatCache structure containing key configuration and metadata
- `nkeys`: Number of key attributes to extract and hash (must be 1-4)
- `tuple`: HeapTuple from which to extract key attribute values

## Dependencies
- Functions called/Symbols referenced:
  - `CatCache`: Structure type containing cache configuration including key column numbers and tuple descriptor
  - `fastgetattr`: High-performance function for extracting attribute values from tuples
  - `CatalogCacheComputeHashValue`: Core hash computation function that combines individual key hashes
- Called from (representative examples):
  - `SearchCatCacheList`: Used when building or searching catalog cache lists
  - `PrepareToInvalidateCacheTuple`: Used during cache invalidation to compute hash of tuples being removed

## Dependencies
- Functions called/Symbols referenced:
  - `CatCache`: Structure type containing cache configuration including key column numbers and tuple descriptor
  - `fastgetattr`: High-performance function for extracting attribute values from tuples
  - `CatalogCacheComputeHashValue`: Core hash computation function that combines individual key hashes
- Called from (representative examples):
  - `SearchCatCacheList`: Used when building or searching catalog cache lists
  - `PrepareToInvalidateCacheTuple`: Used during cache invalidation to compute hash of tuples being removed

## Notes and Other Information
- The function uses a fallthrough switch statement similar to `CatalogCacheComputeHashValue` for efficient key extraction
- Assertions ensure that catalog key attributes are never NULL, which would indicate data corruption
- Uses `fastgetattr` for optimal performance when accessing tuple attributes
- The extracted key values are immediately passed to `CatalogCacheComputeHashValue` for hash computation
- Essential for cache invalidation operations where existing tuples need to be located and removed from the cache
- Supports the same 1-4 key limitation as the underlying hash computation function