# lookup_collation_cache

## Location
src/backend/utils/adt/pg_locale.c: 1253 - 1339

## Overview
lookup_collation_cache is a static function that manages a hash table cache for collation information, storing flags about whether a collation's LC_COLLATE or LC_CTYPE is C/POSIX for performance optimization.

## Definition
```c
static collation_cache_entry *lookup_collation_cache(Oid collation, bool set_flags)
```

## Detailed Description
This function implements a caching mechanism for collation properties to avoid repeated catalog lookups. It maintains a hash table that stores whether each collation's LC_COLLATE and LC_CTYPE settings are equivalent to "C" or "POSIX". The cache is populated lazily - entries are created when first accessed and flags are set only when requested via the set_flags parameter. The function handles different collation providers (BUILTIN and LIBC) with appropriate logic for determining C/POSIX equivalence. The cache persists for the lifetime of a backend process and cannot be flushed, which is acceptable since PostgreSQL doesn't support ALTER COLLATION.

## Parameters / Member Variables
- `collation`: OID of the collation to look up in the cache
- `set_flags`: Boolean indicating whether to populate the collate_is_c and ctype_is_c flags if not already set

## Dependencies
- Functions called/Symbols referenced:
  - hash_create
  - hash_search
  - SearchSysCache1
  - SysCacheGetAttrNotNull
  - TextDatumGetCString
  - ReleaseSysCache
- Called from (representative examples):
  - lc_collate_is_c
  - lc_ctype_is_c
  - pg_newlocale_from_collation

## Notes and Other Information
- The cache is initialized on first use with a hash table capacity of 100 entries
- Cache entries are never invalidated, which is safe since collations cannot be altered
- The function asserts that collation is valid and not DEFAULT_COLLATION_OID
- Different handling for COLLPROVIDER_BUILTIN vs COLLPROVIDER_LIBC collations
- Critical for performance in code paths that frequently check if locale is C/POSIX