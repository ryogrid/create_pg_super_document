# StatisticsObjIsVisibleExt

## Location
[src/backend/catalog/namespace.c:2644-2715](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L2644-L2715)

## Overview
Extended version of StatisticsObjIsVisible that determines whether a statistics object is visible in the current search path, with optional graceful handling of missing statistics objects.

## Definition
```c
static bool StatisticsObjIsVisibleExt(Oid stxid, bool *is_missing)
```

## Detailed Description
StatisticsObjIsVisibleExt performs the actual work of determining statistics object visibility in PostgreSQL's namespace system. It checks if a statistics object identified by OID would be found when searching for the unqualified statistics object name in the current search path.

The function performs a comprehensive visibility check: first, it verifies that the statistics object's namespace is in the active search path, then it walks through the search path to ensure that this specific statistics object would be found first (not shadowed by another statistics object with the same name in an earlier namespace).

The extended version provides graceful error handling - if the statistics object is not found and is_missing is provided, it sets the flag and returns false instead of throwing an error.

## Parameters / Member Variables
- `stxid`: OID of the statistics object to check for visibility  
- `is_missing`: Optional pointer to bool flag; if provided and statistics object is missing, sets *is_missing = true and returns false instead of throwing error

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_statistic_ext (system catalog form)
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)
  - [list_member_oid](../l/list_member_oid.md)
  - SearchSysCacheExists2 (STATEXTNAMENSP cache)
  - [SearchSysCache1](SearchSysCache1.md), ReleaseSysCache
- Called from (representative examples):
  - [StatisticsObjIsVisible](StatisticsObjIsVisible.md)
  - [pg_statistics_obj_is_visible](../p/pg_statistics_obj_is_visible.md)

## Notes and Other Information
- This is a static function, only accessible within namespace.c
- Uses PostgreSQL's system cache (STATEXTOID) for efficient statistics object lookup
- Implements a thorough visibility algorithm: checks namespace membership, then walks search path to detect name conflicts
- The PG_CATALOG_NAMESPACE is always considered to be in the search path
- Part of PostgreSQL's extended statistics system that supports multi-column statistics objects
- Uses SearchSysCacheExists2 to check for name conflicts in earlier namespaces
- Located in src/backend/catalog/namespace.c:2644-2715