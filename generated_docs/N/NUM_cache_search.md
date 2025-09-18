# NUM_cache_search

## Location
src/backend/utils/adt/formatting.c: 5131 - 5151

## Overview
Searches the numeric formatting cache for an existing valid entry matching the given format picture string.

## Definition
static NUMCacheEntry *NUM_cache_search(const char *str)

## Detailed Description
This function performs a linear search through the numeric formatting cache to find an existing entry that matches the provided format string. It only considers valid cache entries and uses string comparison to match the format picture. When a match is found, the function updates the entry's age to the current counter value (implementing LRU cache behavior) and returns a pointer to the cache entry. The function ensures counter overflow protection before updating ages. If no matching entry is found, it returns NULL, indicating that the format string is not currently cached.

## Parameters / Member Variables
- : The numeric format picture string to search for in the cache (null-terminated C string)

## Dependencies
- Functions called/Symbols referenced:
  - NUM_prevent_counter_overflow
  - strcmp
  - NUMCounter (global variable)
  - NUMCache (global array)
  - n_NUMCache (global variable)
  - NUMCacheEntry (struct type)
- Called from (representative examples):
  - NUM_cache_fetch

## Notes and Other Information
- Performs linear search through cache entries - acceptable given small cache size (20 entries)
- Only considers valid cache entries during search
- Updates age on cache hit to maintain LRU ordering
- Returns NULL when no matching entry is found
- Cache hit updates entry age to implement LRU replacement policy
- Part of the numeric formatting cache management system located in src/backend/utils/adt/formatting.c:5131-5151