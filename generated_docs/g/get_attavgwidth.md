# get_attavgwidth

## Location
src/backend/utils/cache/lsyscache.c: 3158 - 3233

## Overview
Retrieves the average width (in bytes) of entries in a specific column from PostgreSQL statistics, which is crucial for query planning and cost estimation.

## Definition
```c
int32 get_attavgwidth(Oid relid, AttrNumber attnum)
```

## Detailed Description
This function looks up the average width of values stored in a specific column by consulting the PostgreSQL statistics system (pg_statistic catalog). The average width is used by the query planner to estimate memory usage, I/O costs, and overall query performance. The function first checks if there's a hook function registered (via `get_attavgwidth_hook`) that can provide the information, allowing extensions to override the default behavior. If no hook provides a result, it performs a system catalog lookup to retrieve the `stawidth` field from the pg_statistic table. The function returns 0 if no statistical data is available for the column.

## Parameters / Member Variables
- `relid`: The OID of the relation (table) containing the column
- `attnum`: The attribute number (column number) within the relation

## Dependencies
- Functions called/Symbols referenced:
  - get_attavgwidth_hook (function pointer)
  - SearchSysCache3
  - ObjectIdGetDatum
  - Int16GetDatum
  - BoolGetDatum
  - HeapTupleIsValid
  - GETSTRUCT
  - ReleaseSysCache
- Called from (representative examples):
  - set_rel_width
  - get_rel_data_width

## Notes and Other Information
- Returns 0 when no statistical data is available for the column
- The hook mechanism allows extensions to provide custom width estimates
- Only consulted for individual tables, not inheritance trees
- Critical for query planner cost estimation, particularly for memory allocation decisions
- The statistics are typically gathered by the ANALYZE command
- Width estimates help determine buffer sizes and join algorithm selection
- Part of the statistics cache system for optimizing repeated lookups
- Located in `src/backend/utils/cache/lsyscache.c:3158-3233`