# make_oper_cache_entry

## Location
src/backend/parser/parse_oper.c: 1020 - 1035

## Overview
Creates and inserts a new cache entry into the operator lookup cache with the specified key and operator OID.

## Definition
```c
static void make_oper_cache_entry(OprCacheKey *key, Oid opr_oid)
```

## Detailed Description
This function adds a new entry to PostgreSQL's operator lookup cache hash table. It takes a cache key (containing operator name, argument types, and search path information) and the resolved operator OID, then stores this mapping in the cache for future lookups. The function assumes the cache hash table has already been initialized and uses hash_search with HASH_ENTER to either find an existing entry or create a new one.

This is typically called after a successful operator lookup to cache the result and avoid repeating expensive catalog searches for the same operator resolution.

## Parameters / Member Variables
- `key`: Pointer to the OprCacheKey structure containing the cache key information
- `opr_oid`: The operator OID to be stored in the cache entry

## Dependencies
- Functions called/Symbols referenced:
  - [hash_search](../h/hash_search.md) (with HASH_ENTER flag)
  - Assert (for debugging validation)
- Called from (representative examples):
  - [oper](../o/oper.md) (main operator lookup function)
  - [left_oper](../l/left_oper.md) (left unary operator lookup)

## Notes and Other Information
- Assumes OprCacheHash has been initialized (checked by Assert)
- Uses HASH_ENTER flag to create new entry or update existing one
- Simple function that performs the final step in operator caching
- Part of the operator resolution performance optimization system
- No return value - operation always succeeds if preconditions are met