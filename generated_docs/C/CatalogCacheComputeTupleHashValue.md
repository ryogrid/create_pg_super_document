# CatalogCacheComputeTupleHashValue

## Location
[src/backend/utils/cache/catcache.c:386-440](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L386-L440)

## Overview
A static function that extracts key attribute values from a HeapTuple and computes the corresponding hash value for catalog cache storage and lookup operations.

## Definition

```c
static uint32
CatalogCacheComputeTupleHashValue(CatCache *cache, int nkeys, HeapTuple tuple)
```
## Detailed Description
The `CatalogCacheComputeTupleHashValue` function serves as a tuple-oriented wrapper around `CatalogCacheComputeHashValue`. It extracts key values from a HeapTuple using the cache's configured key column numbers and tuple descriptor, then delegates to `CatalogCacheComputeHashValue` for the actual hash computation. The function uses `fastgetattr` to efficiently extract attribute values from the tuple and includes assertions to ensure that key attributes are never NULL (which would be a system catalog integrity violation). This function is essential for operations that need to compute hash values for existing tuples, such as cache invalidation and list-based searches where the full tuple is available but individual key values need to be extracted.

## Parameters / Member Variables
- `cache`: Pointer to the CatCache structure containing key configuration and metadata
- `nkeys`: Number of key attributes to extract and hash (must be 1-4)
- `tuple`: HeapTuple from which to extract key attribute values

## Dependencies
- Functions called/Symbols referenced:
  - `[CatCache](CatCache.md)`: Structure type containing cache configuration including key column numbers and tuple descriptor
  - [fastgetattr](../f/fastgetattr.md): High-performance function for extracting attribute values from tuples
  - [CatalogCacheComputeHashValue](CatalogCacheComputeHashValue.md): Core hash computation function that combines individual key hashes
- Called from (representative examples):
  - [SearchCatCacheList](../S/SearchCatCacheList.md): Used when building or searching catalog cache lists
  - [PrepareToInvalidateCacheTuple](../P/PrepareToInvalidateCacheTuple.md): Used during cache invalidation to compute hash of tuples being removed

## Dependencies
- Functions called/Symbols referenced:
  - `[CatCache](CatCache.md)`: Structure type containing cache configuration including key column numbers and tuple descriptor
  - [fastgetattr](../f/fastgetattr.md): High-performance function for extracting attribute values from tuples
  - [CatalogCacheComputeHashValue](CatalogCacheComputeHashValue.md): Core hash computation function that combines individual key hashes
- Called from (representative examples):
  - [SearchCatCacheList](../S/SearchCatCacheList.md): Used when building or searching catalog cache lists
  - [PrepareToInvalidateCacheTuple](../P/PrepareToInvalidateCacheTuple.md): Used during cache invalidation to compute hash of tuples being removed

## Notes and Other Information
- The function uses a fallthrough switch statement similar to `CatalogCacheComputeHashValue` for efficient key extraction
- Assertions ensure that catalog key attributes are never NULL, which would indicate data corruption
- Uses `fastgetattr` for optimal performance when accessing tuple attributes
- The extracted key values are immediately passed to `CatalogCacheComputeHashValue` for hash computation
- Essential for cache invalidation operations where existing tuples need to be located and removed from the cache
- Supports the same 1-4 key limitation as the underlying hash computation function

## Simplified Source

```c
static uint32
CatalogCacheComputeTupleHashValue(CatCache *cache, int nkeys, HeapTuple tuple)
{
    Datum v1 = 0, v2 = 0, v3 = 0, v4 = 0;
    bool isNull = false;
    int *cc_keyno = cache->cc_keyno;
    TupleDesc cc_tupdesc = cache->cc_tupdesc;

    // Extract key values from tuple based on number of keys
    switch (nkeys)
    {
        case 4:
            v4 = fastgetattr(tuple, cc_keyno[3], cc_tupdesc, &isNull);
            Assert(!isNull);
            // FALLTHROUGH
        case 3:
            v3 = fastgetattr(tuple, cc_keyno[2], cc_tupdesc, &isNull);
            Assert(!isNull);
            // FALLTHROUGH
        case 2:
            v2 = fastgetattr(tuple, cc_keyno[1], cc_tupdesc, &isNull);
            Assert(!isNull);
            // FALLTHROUGH
        case 1:
            v1 = fastgetattr(tuple, cc_keyno[0], cc_tupdesc, &isNull);
            Assert(!isNull);
            break;
        default:
            elog(FATAL, "wrong number of hash keys: %d", nkeys);
            break;
    }

    // Compute and return hash value using extracted key values
    return CatalogCacheComputeHashValue(cache, nkeys, v1, v2, v3, v4);
}
```