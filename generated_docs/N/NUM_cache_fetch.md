# NUM_cache_fetch

## Location
src/backend/utils/adt/formatting.c: 5152 - 5179

## Overview
High-level interface to retrieve a parsed numeric format cache entry, either from existing cache or by parsing and caching a new format string.

## Definition
static NUMCacheEntry *NUM_cache_fetch(const char *str)

## Detailed Description
This function serves as the primary interface for obtaining a cached numeric format entry. It implements a cache-first strategy: first attempting to find an existing valid cache entry using NUM_cache_search(), and if not found, creating a new entry via NUM_cache_getnew(). For new entries, it performs the complete parsing workflow: initializes the format descriptor (NUMDesc) to zero state using zeroize_NUM(), parses the format string into FormatNode structures using parse_format() with numeric formatting keywords and flags, and finally marks the entry as valid. This function guarantees that returned cache entries are fully parsed and ready for use in numeric formatting operations.

## Parameters / Member Variables
- : The numeric format picture string to fetch from cache or parse (null-terminated C string)

## Dependencies
- Functions called/Symbols referenced:
  - [NUM_cache_search](NUM_cache_search.md)
  - [NUM_cache_getnew](NUM_cache_getnew.md)
  - zeroize_NUM
  - parse_format
  - NUM_keywords (global array)
  - NUM_index (global array)
  - NUM_FLAG (constant)
  - NUMCacheEntry (struct type)
- Called from (representative examples):
  - [NUM_cache](NUM_cache.md)

## Notes and Other Information
- Implements cache-first strategy for performance optimization
- Ensures all returned entries are fully parsed and marked as valid
- Handles the complete lifecycle of cache entry creation and parsing
- Uses NUM_keywords, NUM_index, and NUM_FLAG for numeric format parsing
- Critical function for numeric formatting performance as it manages the boundary between cached and uncached operations
- Always returns a valid, ready-to-use cache entry
- Part of the numeric formatting cache management system located in src/backend/utils/adt/formatting.c:5152-5179