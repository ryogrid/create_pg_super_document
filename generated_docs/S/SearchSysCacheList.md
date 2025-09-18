# SearchSysCacheList

## Location
src/backend/utils/cache/syscache.c: 679 - 698

## Overview
SearchSysCacheList is a PostgreSQL system cache function that performs list-based searches to find all cached catalog tuples matching a partial key, returning them as a catclist structure.

## Definition


## Detailed Description
SearchSysCacheList provides a list-search interface for PostgreSQL's system catalog cache system. Unlike SearchSysCache functions that return individual tuples, this function returns a list of all tuples that match the specified partial key criteria. It serves as a high-level wrapper around the lower-level SearchCatCacheList function, providing cache ID validation and error handling.

The function is designed for scenarios where you need to find multiple related catalog entries that share common key values in the first K columns of a cache's key structure. For example, finding all columns belonging to a specific table, or all functions in a particular namespace.

The function validates the cache ID to ensure it refers to a valid system cache, then delegates the actual list search operation to SearchCatCacheList, which handles the complex logic of cache lookup, tuple retrieval from system catalogs when necessary, and maintaining reference counts for memory management.

## Parameters / Member Variables
- : Integer identifier specifying which system cache to search (must be valid cache ID between 0 and SysCacheSize-1)
- : Number of key values to use for the search (must be less than the cache's total number of keys)  
- : First key value for the search criteria (Datum type)
- : Second key value for the search criteria (Datum type, can be 0 if not used)
- : Third key value for the search criteria (Datum type, can be 0 if not used)

## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid (for cache validation)
  - [SearchCatCacheList](SearchCatCacheList.md) (core list search implementation)
  - elog (for error reporting)
- Called from (representative examples):
  - SearchSysCacheList1 (convenience macro for 1-key searches)
  - SearchSysCacheList2 (convenience macro for 2-key searches)  
  - SearchSysCacheList3 (convenience macro for 3-key searches)

## Notes and Other Information
- This function performs cache ID bounds checking and throws an ERROR if an invalid cache ID is provided
- The returned catclist structure must be released using ReleaseCatCacheList() when no longer needed
- Convenience macros SearchSysCacheList1, SearchSysCacheList2, and SearchSysCacheList3 are provided for type safety and clarity
- The function is part of PostgreSQL's system catalog caching mechanism, which significantly improves performance by avoiding repeated catalog table scans
- Located in src/backend/utils/cache/syscache.c:679-698
- Declared in src/include/utils/syscache.h:77
- Users must include both syscache.h and catcache.h to use this function since it returns a catclist structure