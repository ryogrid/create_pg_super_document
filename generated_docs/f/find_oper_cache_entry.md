# find_oper_cache_entry

## Location
src/backend/parser/parse_oper.c: 981 - 1019

## Overview
Searches for an existing cache entry matching the given operator lookup key and returns the cached operator OID if found.

## Definition
```c
static Oid find_oper_cache_entry(OprCacheKey *key)
```

## Detailed Description
This function implements the lookup mechanism for PostgreSQL's operator cache system. It searches the global operator cache hash table (OprCacheHash) for an entry matching the provided key. If this is the first time the function is called, it initializes the hash table and registers invalidation callbacks to ensure cache consistency when system catalogs change.

The function performs lazy initialization of the cache hash table, creating it with 256 initial buckets and registering callbacks for pg_operator and pg_cast system catalog changes to maintain cache validity.

## Parameters / Member Variables
- `key`: Pointer to the OprCacheKey structure containing the search criteria (operator name, argument types, search path)

## Dependencies
- Functions called/Symbols referenced:
  - hash_create
  - CacheRegisterSyscacheCallback  
  - InvalidateOprCacheCallBack
  - hash_search
- Called from (representative examples):
  - oper (main operator lookup function)
  - left_oper (left unary operator lookup)

## Notes and Other Information
- Returns InvalidOid if no matching cache entry is found
- Performs lazy initialization of the operator cache hash table on first call
- Uses HASH_ELEM and HASH_BLOBS flags for efficient binary key comparison
- Registers invalidation callbacks for OPERNAMENSP and CASTSOURCETARGET syscaches
- Cache size starts at 256 buckets and grows as needed
- Critical for performance optimization in operator resolution