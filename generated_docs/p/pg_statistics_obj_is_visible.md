# pg_statistics_obj_is_visible

## Location
[src/backend/catalog/namespace.c:5006-5019](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L5006-L5019)

## Overview
Determines whether a given extended statistics object is visible in the current search path, returning NULL if the statistics object does not exist.

## Definition
```c
Datum pg_statistics_obj_is_visible(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL SQL-callable function that checks the visibility of an extended statistics object identified by its OID in the current search path. It serves as a wrapper around the internal StatisticsObjIsVisibleExt function, providing a SQL interface for statistics object visibility checks. The function returns a boolean value indicating whether the statistics object is accessible from the current namespace context, or NULL if the statistics object doesn't exist in the system catalogs.

The visibility check considers the current search path and ensures that the statistics object would be found by name resolution. A statistics object is considered visible if it exists in a namespace that's in the current search path and wouldn't be shadowed by another statistics object with the same name in an earlier namespace. Extended statistics objects are used by PostgreSQL's query planner to collect multi-column statistics for better query optimization.

## Parameters / Member Variables
- First argument (OID): The object identifier of the extended statistics object to check for visibility

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID: Extracts OID argument from function call
  - [StatisticsObjIsVisibleExt](../S/StatisticsObjIsVisibleExt.md): Internal function that performs the actual visibility check
  - PG_RETURN_NULL: Returns NULL value to SQL caller
  - PG_RETURN_BOOL: Returns boolean value to SQL caller
- Called from (representative examples):
  - Available as SQL function pg_statistics_obj_is_visible()

## Notes and Other Information
- This is a system information function available in SQL as pg_statistics_obj_is_visible(oid)
- Returns NULL rather than FALSE when the statistics object doesn't exist, following PostgreSQL's convention for visibility functions
- The function uses the is_missing parameter of StatisticsObjIsVisibleExt to distinguish between "not visible" and "doesn't exist"
- Part of PostgreSQL's namespace and visibility system for schema-qualified object resolution
- Extended statistics objects (CREATE STATISTICS) are used to collect multi-column statistics for improved query planning
- Located in src/backend/catalog/namespace.c:5006-5019

## Simplified Source

```c
Datum pg_statistics_obj_is_visible(PG_FUNCTION_ARGS) {
    Oid stats_obj_oid = PG_GETARG_OID(0);
    bool is_missing = false;

    // Check if statistics object is visible in current search path
    bool result = StatisticsObjIsVisibleExt(stats_obj_oid, &is_missing);

    // Return NULL if statistics object doesn't exist, otherwise return visibility status
    if (is_missing)
        PG_RETURN_NULL();
    PG_RETURN_BOOL(result);
}
```