# GetSysCacheOid

## Location
[src/backend/utils/cache/syscache.c:449-480](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/syscache.c#L449-L480)

## Overview
GetSysCacheOid is a convenience function that searches the system cache for a tuple and extracts an OID value from a specified column of the found tuple.

## Definition
```c
Oid GetSysCacheOid(int cacheId, AttrNumber oidcol, Datum key1, Datum key2, Datum key3, Datum key4)
```

## Detailed Description
This function combines cache searching with OID extraction in a single operation. It searches for a tuple in the specified system cache using the provided keys, and if found, extracts the OID value from the specified column. The function is designed for scenarios where the caller needs to retrieve a specific OID from a catalog tuple without needing access to the full tuple contents.

The function performs a SearchSysCache operation, validates that a tuple was found, uses heap_getattr to extract the OID value from the specified column, and then releases the cache entry. If no tuple is found, it returns InvalidOid. The function includes an assertion to ensure that the specified column is never NULL, as OID columns in system catalogs should always contain valid values.

## Parameters / Member Variables
- `cacheId`: The identifier of the system cache to search in (corresponds to entries in SysCacheIdentifier enum)
- `oidcol`: The column number (AttrNumber) from which to extract the OID value
- `key1`: The first search key value
- `key2`: The second search key value (can be unused depending on cache structure)
- `key3`: The third search key value (can be unused depending on cache structure)
- `key4`: The fourth search key value (can be unused depending on cache structure)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache](../S/SearchSysCache.md)
  - HeapTupleIsValid
  - [heap_getattr](../h/heap_getattr.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - SysCache (global array access)
- Called from (representative examples):
  - GetSysCacheOid1 (macro wrapper for single key searches)
  - GetSysCacheOid2 (macro wrapper for two key searches)
  - GetSysCacheOid3 (macro wrapper for three key searches)
  - GetSysCacheOid4 (macro wrapper for four key searches)

## Notes and Other Information
- Returns the OID value from the specified column if a matching tuple is found, or InvalidOid if no tuple exists
- This function is typically accessed through convenience macros (GetSysCacheOid1, GetSysCacheOid2, etc.) that provide the appropriate number of keys for specific cache types
- The function includes an assertion that the extracted OID column is not NULL, reflecting the expectation that OID columns in system catalogs always contain valid values
- No locks are retained on the cache entry after the function completes
- The oidcol parameter uses AttrNumber type, which is a 1-based column numbering system
- Commonly used to retrieve object identifiers from system catalog tables efficiently