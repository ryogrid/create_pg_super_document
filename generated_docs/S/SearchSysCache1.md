# SearchSysCache1

## Location
[src/backend/utils/cache/syscache.c:221-231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/syscache.c#L221-L231)

## Overview
A specialized wrapper around SearchCatCache1 optimized for system catalog caches that use exactly one search key.

## Definition


## Detailed Description
SearchSysCache1 is a type-safe convenience function that provides access to system catalog caches that are indexed by a single key. It serves as a specialized version of SearchSysCache, but with compile-time enforcement that the target cache uses exactly one search key.

The function validates that the specified cache is configured for single-key lookups through an assertion check (cc_nkeys == 1), providing additional safety compared to the general SearchSysCache function. This helps prevent runtime errors when attempting to use single-key searches on multi-key caches.

Like other SearchSysCache variants, it returns a read-only cache copy of the tuple that must be released with ReleaseSysCache() when no longer needed.

## Parameters / Member Variables
- : Integer identifier specifying which system cache to search (must be valid single-key cache)
- : The single search key value used to locate the desired tuple

## Dependencies
- Functions called/Symbols referenced:
  - PointerIsValid
  - [SearchCatCache1](SearchCatCache1.md)
- Called from (representative examples):
  - No direct references found in the current codebase analysis

## Notes and Other Information
- Enforces single-key cache constraint through assertion (cc_nkeys == 1)
- More type-safe than using SearchSysCache with unused key2, key3, key4 parameters
- Provides better documentation of intent when searching single-key caches
- The returned tuple is a cache copy and must NOT be freed by the caller
- Must call ReleaseSysCache() when finished with the returned tuple
- Part of a family of type-safe cache search functions (SearchSysCache1, SearchSysCache2, etc.)
- Helps catch programming errors at runtime through key count validation