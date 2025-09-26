# DCH_cache_search

## Location
[src/backend/utils/adt/formatting.c:4133-4153](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L4133-L4153)

## Overview
Searches the DCH cache for an existing entry that matches the specified format pattern string and standard flag, updating its access time if found.

## Definition
```c
static DCHCacheEntry *DCH_cache_search(const char *str, bool std)
```

## Detailed Description
This function implements the cache lookup mechanism for the DCH (Date/Time formatting Cache) system. It performs a linear search through all valid cache entries, comparing both the format string and the standard flag for exact matches. When a matching entry is found, the function implements a \"touch\" operation by updating the entry's age to the current DCHCounter value, effectively marking it as recently accessed for LRU (Least Recently Used) cache management.

The search is comprehensive, requiring both the format string (`str`) and the standard flag (`std`) to match exactly. This dual-key matching ensures that standard and non-standard interpretations of the same format string are cached separately, preventing format confusion.

Before performing any counter updates, the function proactively calls DCH_prevent_counter_overflow() to ensure the aging system remains within safe integer bounds.

## Parameters / Member Variables
- `str`: The format pattern string to search for (must match exactly)
- `std`: Boolean flag indicating standard format interpretation (must match exactly)

## Dependencies
- Functions called/Symbols referenced:
  - DCH_prevent_counter_overflow (counter overflow protection)
  - DCHCacheEntry (structure type)
  - strcmp (string comparison)
  - DCHCache (global cache array)
  - n_DCHCache (global cache size variable)
  - DCHCounter (global age counter)
- Called from:
  - DCH_cache_fetch

## Notes and Other Information
- Uses linear search algorithm with O(n) complexity where n is the number of cached entries
- Only searches valid entries (entries with valid=true)
- Updates entry age on cache hit to implement LRU replacement policy
- Returns NULL if no matching entry is found
- The dual-key search (string + std flag) ensures format interpretation consistency
- Cache hits are relatively efficient due to the bounded cache size (DCH_CACHE_ENTRIES)
- Forms part of the three-function cache management system alongside DCH_cache_getnew and DCH_cache_fetch