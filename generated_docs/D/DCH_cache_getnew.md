# DCH_cache_getnew

## Location
src/backend/utils/adt/formatting.c: 4073 - 4132

## Overview
Selects and prepares a DCHCacheEntry to hold a given format pattern string, either by recycling an existing entry or allocating a new one.

## Definition
```c
static DCHCacheEntry *DCH_cache_getnew(const char *str, bool std)
```

## Detailed Description
This function implements the cache entry allocation strategy for the DCH (Date/Time formatting Cache) system. It employs a two-phase approach:

**Phase 1 - Cache Full Scenario**: When the cache has reached its maximum capacity (DCH_CACHE_ENTRIES), the function searches for the optimal entry to recycle. It prioritizes invalid entries first, then falls back to the oldest valid entry (lowest age value). The selected entry is invalidated, updated with the new format string, and assigned a new age value.

**Phase 2 - Cache Growth Scenario**: When there's available space, the function allocates a new DCHCacheEntry in TopMemoryContext (ensuring it persists across transactions), initializes it with the provided format string and parameters, and increments the cache count.

The function ensures cache counter overflow protection by calling DCH_prevent_counter_overflow() before manipulating age values. All returned entries are initially marked as invalid, leaving the caller responsible for populating the format field and setting the valid flag.

## Parameters / Member Variables
- `str`: The format pattern string to be cached (copied into the cache entry)
- `std`: Boolean flag indicating whether this is a standard format (stored in the cache entry for future reference)

## Dependencies
- Functions called/Symbols referenced:
  - DCH_prevent_counter_overflow (counter overflow protection)
  - DCHCacheEntry (structure type)
  - DCH_CACHE_ENTRIES (maximum cache size constant)  
  - MemoryContextAllocZero (memory allocation)
  - TopMemoryContext (persistent memory context)
  - strlcpy (safe string copying)
  - DCH_CACHE_SIZE (maximum format string length constant)
- Called from:
  - DCH_cache_fetch

## Notes and Other Information
- Uses aggressive aging strategy where every access increments the global DCHCounter
- Memory allocation uses TopMemoryContext to ensure entries survive transaction boundaries
- Debug logging available when DEBUG_TO_FROM_CHAR is defined
- The LRU (Least Recently Used) replacement policy helps maintain frequently used formats in cache
- Returned entries require caller to populate the format field and set valid=true to complete initialization
- Cache size is bounded by DCH_CACHE_ENTRIES constant to prevent unbounded memory growth