# OprCacheEntry

## Location
src/backend/parser/parse_oper.c: 59 - 65

## Overview
OprCacheEntry is a hash table entry structure that stores cached operator resolution results in PostgreSQL's parser, combining the lookup key with the resolved operator OID.

## Definition
```c
typedef struct OprCacheEntry
{
	/* the hash lookup key MUST BE FIRST */
	OprCacheKey key;

	Oid			opr_oid;		/* OID of the resolved operator */
} OprCacheEntry;
```

## Detailed Description
OprCacheEntry represents a complete cache entry in PostgreSQL's operator lookup cache. It combines an OprCacheKey (containing operator name, argument types, and search path) with the resolved operator OID. This structure is used in a hash table to cache the results of operator resolution, avoiding repeated expensive catalog lookups during query parsing. The key field must be first in the structure as required by PostgreSQL's hash table implementation for proper key-based lookups.

## Parameters / Member Variables
- `key`: OprCacheKey structure containing the lookup key (operator name, argument types, and search path) - must be first field for hash table compatibility
- `opr_oid`: OID of the resolved operator from the pg_operator catalog

## Dependencies
- Functions called/Symbols referenced:
  - OprCacheKey
- Called from (representative examples):
  - find_oper_cache_entry
  - make_oper_cache_entry
  - InvalidateOprCacheCallBack

## Notes and Other Information
- The hash lookup key (OprCacheKey) must be the first field in the structure for PostgreSQL's hash table implementation to work correctly
- Used in conjunction with OprCacheHash, a hash table that maps operator signatures to their resolved OIDs
- Cache entries are invalidated when pg_operator or pg_cast system catalogs change
- Provides significant performance improvement by avoiding repeated operator resolution during query parsing
- Located in src/backend/parser/parse_oper.c:59-65