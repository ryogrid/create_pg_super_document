# InitializeRelfilenumberMap

## Location
[src/backend/utils/cache/relfilenumbermap.c:86-140](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relfilenumbermap.c#L86-L140)

## Overview
InitializeRelfilenumberMap is a static initialization function that sets up the relfilenumber-to-relid mapping cache, either on first use or after a reset.

## Definition
```c
static void InitializeRelfilenumberMap(void)
```

## Detailed Description
This function performs the complete initialization of the relfilenumber mapping system. It sets up the hash table infrastructure used to cache mappings between relfilenumber (tablespace OID + filenode) and relation OIDs. The initialization process includes:

1. Ensuring CacheMemoryContext is available for memory allocation
2. Setting up scan keys for pg_class lookups using tablespace and relfilenode attributes
3. Creating the RelfilenumberMapHash hash table with appropriate configuration
4. Registering an invalidation callback to maintain cache consistency

The function uses a two-element scan key array to efficiently query pg_class by both reltablespace and relfilenode attributes, enabling fast reverse lookups from file identifiers to relation OIDs.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - CreateCacheMemoryContext
  - MemSet
  - fmgr_info_cxt
  - hash_create
  - CacheRegisterRelcacheCallback
  - RelfilenumberMapInvalidateCallback
  - HASHCTL
  - RelfilenumberMapKey
  - RelfilenumberMapEntry
  - HASH_ELEM, HASH_BLOBS, HASH_CONTEXT
- Called from (representative examples):
  - RelidByRelfilenumber

## Notes and Other Information
- This is a static function only used within the relfilenumbermap.c module
- Initializes global variables: RelfilenumberMapHash and relfilenumber_skey
- Uses CacheMemoryContext for persistent cache storage across transactions  
- Hash table is created with initial capacity of 64 entries
- Scan keys are configured for equality searches on pg_class.reltablespace and pg_class.relfilenode
- Critical for establishing the reverse mapping infrastructure from file identifiers to relation OIDs
- Registers RelfilenumberMapInvalidateCallback to handle cache invalidation events
- Delayed hash table creation prevents partial initialization on memory errors