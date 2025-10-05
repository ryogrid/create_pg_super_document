# NUM_cache_getnew

## Location
[src/backend/utils/adt/formatting.c:5072-5130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L5072-L5130)

## Overview
Allocates or recycles a NUMCacheEntry to hold a numeric format picture string, managing cache overflow through LRU replacement strategy.

## Definition
static NUMCacheEntry *NUM_cache_getnew(const char *str)

## Detailed Description
This function manages the allocation of cache entries in the numeric formatting cache system. It implements a hybrid cache management strategy that handles both cache growth and replacement. When the cache has available slots, it allocates new entries using PostgreSQL's memory context system. When the cache is full (NUM_CACHE_ENTRIES limit reached), it employs a Least Recently Used (LRU) replacement algorithm, with preference given to invalid entries. The function ensures counter overflow protection by calling NUM_prevent_counter_overflow() before incrementing the global age counter. Each returned entry has its validity flag cleared and age updated, requiring the caller to populate the format parsing results before marking it valid.

## Parameters / Member Variables
- : The numeric format picture string to be cached (null-terminated C string)

## Dependencies
- Functions called/Symbols referenced:
  - [NUM_prevent_counter_overflow](NUM_prevent_counter_overflow.md)
  - [strlcpy](../s/strlcpy.md)
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - TopMemoryContext
  - NUMCounter (global variable)
  - NUMCache (global array)
  - n_NUMCache (global variable)
  - NUM_CACHE_ENTRIES (constant)
  - NUM_CACHE_SIZE (constant)
- Called from (representative examples):
  - [NUM_cache_fetch](NUM_cache_fetch.md)

## Notes and Other Information
- Returns cache entries in an invalid state - caller must populate format and Num fields, then set valid = true
- Uses TopMemoryContext for allocation to ensure cache entries persist across transactions
- Implements LRU replacement but prioritizes invalid entries for recycling
- Cache size is limited to NUM_CACHE_ENTRIES (20) entries
- Each cache entry can hold format strings up to NUM_CACHE_SIZE characters
- Debug logging available when DEBUG_TO_FROM_CHAR is defined
- Part of the numeric formatting cache management system located in src/backend/utils/adt/formatting.c:5072-5130

## Simplified Source

```c
static NUMCacheEntry *NUM_cache_getnew(const char *str) {
    NUMCacheEntry *ent;

    // Prevent counter overflow before incrementing
    NUM_prevent_counter_overflow();

    // Handle cache overflow by finding oldest/invalid entry to recycle
    if (n_NUMCache >= NUM_CACHE_ENTRIES) {
        NUMCacheEntry *old = NUMCache[0];

        // Find best candidate: invalid entry if available, otherwise oldest
        if (old->valid) {
            for (int i = 1; i < NUM_CACHE_ENTRIES; i++) {
                ent = NUMCache[i];
                if (!ent->valid) {
                    old = ent;
                    break;
                }
                if (ent->age < old->age)
                    old = ent;
            }
        }

        // Recycle the selected entry
        old->valid = false;
        strlcpy(old->str, str, NUM_CACHE_SIZE + 1);
        old->age = (++NUMCounter);
        return old;
    }
    else {
        // Allocate new cache entry
        ent = (NUMCacheEntry *) MemoryContextAllocZero(TopMemoryContext,
                                                       sizeof(NUMCacheEntry));
        NUMCache[n_NUMCache] = ent;
        ent->valid = false;
        strlcpy(ent->str, str, NUM_CACHE_SIZE + 1);
        ent->age = (++NUMCounter);
        ++n_NUMCache;
        return ent;
    }
}
```