# injection_point_cache_add

## Location
src/backend/utils/misc/injection_point.c: 117 - 161

## Overview
Adds an injection point entry to the local backend cache, creating a hash table if this is the first injection point being cached.

## Definition
```c
static InjectionPointCacheEntry *
injection_point_cache_add(const char *name,
                         int slot_idx,
                         uint64 generation,
                         InjectionPointCallback callback,
                         const void *private_data)
```

## Detailed Description
This function manages a process-local cache of injection point callbacks that have been loaded from shared memory. It creates a hash table on first use (stored in TopMemoryContext) and adds new injection point entries to this cache. The cache stores the callback function pointer, private data, and metadata about the shared memory slot for validation purposes. This local caching mechanism helps avoid repeated lookups in shared memory for frequently used injection points.

## Parameters / Member Variables
- `name`: Name of the injection point (up to INJ_NAME_MAXLEN=64 characters)
- `slot_idx`: Index of the corresponding slot in shared memory
- `generation`: Generation number from shared memory slot used for cache validation
- `callback`: Function pointer to the injection point callback (type: InjectionPointCallback)
- `private_data`: Private data buffer to be copied into cache entry (up to INJ_PRIVATE_MAXLEN=1024 bytes)

## Dependencies
- Functions called/Symbols referenced:
  - hash_create: Creates the hash table on first use
  - hash_search: Searches/inserts entries in the hash table with HASH_ENTER flag
  - strlcpy: Copies the injection point name
  - memcpy: Copies private data buffer
- Called from (representative examples):
  - injection_point_cache_load: Loads injection points from shared memory into cache

## Notes and Other Information
- The function is static (internal to injection_point.c)
- Hash table is created with HASH_ELEM | HASH_STRINGS | HASH_CONTEXT flags
- Uses Assert(\!found) to ensure no duplicate entries are added
- Cache is stored in TopMemoryContext to persist across transactions
- Maximum number of injection points is limited by MAX_INJECTION_POINTS constant