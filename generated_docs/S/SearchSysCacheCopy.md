# SearchSysCacheCopy

## Location
src/backend/utils/cache/syscache.c: 380 - 404

## Overview
SearchSysCacheCopy is a convenience function that searches the system cache and returns a modifiable copy of the found tuple.

## Definition
HeapTuple SearchSysCacheCopy(int cacheId, Datum key1, Datum key2, Datum key3, Datum key4)

## Detailed Description
SearchSysCacheCopy combines system cache search with tuple copying in a single operation. It searches for a tuple using the provided keys, and if found, creates a modifiable copy using heap_copytuple() before releasing the original cached tuple. This is useful when code needs to modify a system catalog tuple, as the cached versions are read-only. The function handles the entire lifecycle of search, copy, and release automatically.

## Parameters / Member Variables
- cacheId: Integer identifier of the system cache to search in
- key1: First search key value as a Datum (may be unused depending on cache)
- key2: Second search key value as a Datum (may be unused depending on cache)
- key3: Third search key value as a Datum (may be unused depending on cache)
- key4: Fourth search key value as a Datum (may be unused depending on cache)

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache
  - heap_copytuple
  - ReleaseSysCache (called internally by the function)
- Called from (representative examples):
  - SearchSysCacheCopy1 (macro wrapper)
  - SearchSysCacheCopy2 (macro wrapper)
  - SearchSysCacheCopy3 (macro wrapper)
  - SearchSysCacheCopy4 (macro wrapper)

## Notes and Other Information
- Returns a modifiable copy that must be freed with heap_freetuple() when no longer needed
- Automatically handles releasing the original cached tuple, so no ReleaseSysCache() call is needed
- The function accepts up to 4 keys but delegates to SearchSysCache which handles variable key counts
- Returns NULL if no matching tuple is found in the cache
- Essential for code that needs to modify system catalog information
- More convenient than manual search-copy-release sequences