# ConversionIsVisibleExt

## Location
src/backend/catalog/namespace.c: 2521 - 2574

## Overview
Extended version of ConversionIsVisible that determines whether a conversion is visible in the current search path, with optional graceful handling of missing conversions.

## Definition
```c
static bool ConversionIsVisibleExt(Oid conid, bool *is_missing)
```

## Detailed Description
ConversionIsVisibleExt performs the actual work of determining conversion visibility in PostgreSQL's namespace system. It checks if a conversion identified by OID would be found when searching for the unqualified conversion name in the current search path.

The function performs a two-stage visibility check: first, it verifies that the conversion's namespace is in the active search path, then it ensures that no other conversion with the same name appears earlier in the search path (which would shadow this conversion).

The extended version provides graceful error handling - if the conversion is not found and is_missing is provided, it sets the flag and returns false instead of throwing an error.

## Parameters / Member Variables
- `conid`: OID of the conversion to check for visibility
- `is_missing`: Optional pointer to bool flag; if provided and conversion is missing, sets *is_missing = true and returns false instead of throwing error

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_conversion (system catalog form)
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)
  - [list_member_oid](../l/list_member_oid.md)
  - [ConversionGetConid](ConversionGetConid.md)
  - [SearchSysCache1](../S/SearchSysCache1.md), ReleaseSysCache
- Called from (representative examples):
  - [ConversionIsVisible](ConversionIsVisible.md)
  - [pg_conversion_is_visible](../p/pg_conversion_is_visible.md)

## Notes and Other Information
- This is a static function, only accessible within namespace.c
- Uses PostgreSQL's system cache (CONVOID) for efficient conversion lookup
- Implements the standard PostgreSQL visibility algorithm: check namespace membership, then check for name conflicts
- The PG_CATALOG_NAMESPACE is always considered to be in the search path
- Part of PostgreSQL's comprehensive namespace visibility system that ensures proper object resolution
- Located in src/backend/catalog/namespace.c:2521-2574