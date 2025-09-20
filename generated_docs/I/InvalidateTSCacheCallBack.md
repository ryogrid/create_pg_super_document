# InvalidateTSCacheCallBack

## Location
[src/backend/utils/cache/ts_cache.c:94-112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/ts_cache.c#L94-L112)

## Overview
A callback function that invalidates text search cache entries when changes are detected in the PostgreSQL text search system catalog, ensuring cache consistency across backends.

## Definition

```c
static void
InvalidateTSCacheCallBack(Datum arg, int cacheid, uint32 hashvalue)
```
## Detailed Description
InvalidateTSCacheCallBack is a syscache callback function designed to detect when visible changes have been made to text search catalog entries, either by the current backend or another one. Rather than attempting to flush only the specific cache entry that changed, this function takes a simpler approach and invalidates all entries in the related hash table, which is reasonable given that text search configuration changes are typically infrequent.

The function can be used for all text search caches by passing the appropriate hash table address as the "arg" parameter. It iterates through all entries in the specified hash table and marks them as invalid by setting their isvalid flag to false. Additionally, if the hash table being invalidated is the TSConfigCacheHash, it also invalidates the current configuration cache by setting TSCurrentConfigCache to InvalidOid.

## Parameters / Member Variables
- : A Datum containing a pointer to the hash table (HTAB*) to be invalidated
- : The system catalog cache identifier (not directly used in the function)
- : The hash value of the changed entry (not directly used in the function)

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - [HTAB](../H/HTAB.md) (hash table structure)
  - HASH_SEQ_STATUS (hash sequence status structure)
  - TSAnyCacheEntry (base cache entry structure)
  - TSConfigCacheHash (global hash table variable)
  - TSCurrentConfigCache (global current config cache variable)
- Called from (representative examples):
  - [lookup_ts_parser_cache](../l/lookup_ts_parser_cache.md) (registers this callback)
  - [lookup_ts_dictionary_cache](../l/lookup_ts_dictionary_cache.md) (registers this callback)
  - [init_ts_config_cache](../i/init_ts_config_cache.md) (registers this callback)

## Notes and Other Information
- This is a static function, meaning it's only visible within the ts_cache.c compilation unit
- The function uses a generic approach that works for all text search cache types by accepting the hash table as a parameter
- The decision to invalidate all entries rather than specific ones is a design trade-off that prioritizes simplicity over fine-grained cache management
- Special handling exists for the configuration cache (TSConfigCacheHash) where an additional global cache variable is invalidated
- The cacheid and hashvalue parameters are part of the standard syscache callback interface but are not used by this particular implementation