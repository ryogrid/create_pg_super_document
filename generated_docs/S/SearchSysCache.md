# SearchSysCache

## Location
src/backend/utils/cache/syscache.c: 208 - 220

## Overview
A convenience wrapper around SearchCatCache that provides a simplified interface for searching PostgreSQL's system catalog caches with up to 4 search keys.

## Definition


## Detailed Description
SearchSysCache serves as a high-level interface to PostgreSQL's catalog cache system, abstracting away the complexity of cache initialization and key management. It acts as a layer on top of SearchCatCache, automatically handling the cache lookup and reference counting.

The function returns a cache copy of the requested tuple if found, or NULL if no matching tuple exists. The returned tuple is a read-only cache copy that must NOT be modified by the caller. The function automatically increments the reference count for the returned tuple, which must be decremented by calling ReleaseSysCache() when the caller is finished with the tuple.

This is the primary interface used throughout PostgreSQL for accessing cached system catalog information, providing a standardized way to query system metadata.

## Parameters / Member Variables
- : Integer identifier specifying which system cache to search (must be valid cache ID)
- : First search key value (Datum type for flexibility)
- : Second search key value (can be unused if cache uses fewer keys)
- : Third search key value (can be unused if cache uses fewer keys)  
- : Fourth search key value (can be unused if cache uses fewer keys)

## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid
  - SearchCatCache
- Called from (representative examples):
  - SearchSysCacheCopy
  - SearchSysCacheExists
  - GetSysCacheOid

## Notes and Other Information
- The returned tuple is a cache copy and must NEVER be freed by the caller
- Always call ReleaseSysCache() when done with the returned tuple to avoid reference count leaks
- The function validates the cacheId parameter with assertions to ensure cache validity
- Supports up to 4 search keys, with unused keys typically set to appropriate null/default values
- The tuple remains locked in cache until ReleaseSysCache() is called or transaction ends
- This is the most commonly used function for system catalog lookups in PostgreSQL
- Part of the performance-critical path for metadata access in query processing