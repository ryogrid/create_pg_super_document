# get_attstatsslot

## Location
src/backend/utils/cache/lsyscache.c: 3234 - 3343

## Overview
Extracts the contents of a specific "slot" from a pg_statistic tuple, providing access to statistical data used by the query planner for selectivity estimation and cost calculations.

## Definition
```c
bool get_attstatsslot(AttStatsSlot *sslot, HeapTuple statstuple, int reqkind, Oid reqop, int flags)
```

## Detailed Description
This function extracts statistical information from a specific slot within a pg_statistic tuple. PostgreSQL stores various types of statistics (histogram, most common values, correlation, etc.) in numbered slots within each statistic entry. The function searches for a slot matching the requested kind and operator, then extracts the statistical values and/or numbers based on the provided flags. It handles both the stavalues (actual data values) and stanumbers (floating-point statistical measures) arrays, properly detoasting and copying them for the caller's use. The function is designed to work with already-looked-up tuples to avoid redundant cache lookups when multiple statistics are needed from the same entry.

## Parameters / Member Variables
- `sslot`: Pointer to output structure that receives the extracted statistical data
- `statstuple`: HeapTuple from pg_statistic cache containing the statistics
- `reqkind`: STAKIND code specifying the type of statistics slot desired (e.g., histogram, MCV)
- `reqop`: STAOP value for the desired operator, or InvalidOid if any operator is acceptable
- `flags`: Bitmask of ATTSTATSSLOT_VALUES and/or ATTSTATSSLOT_NUMBERS indicating which data to extract

## Dependencies
- Functions called/Symbols referenced:
  - memset
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md)
  - DatumGetArrayTypePCopy
  - ARR_ELEMTYPE
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - GETSTRUCT
  - [deconstruct_array](../d/deconstruct_array.md)
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - ARR_DIMS, ARR_NDIM, ARR_HASNULL
  - ARR_DATA_PTR
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [var_eq_const](../v/var_eq_const.md)
  - histogram_selectivity
  - mcv_selectivity
  - [eqjoinsel](../e/eqjoinsel.md)
  - [calc_rangesel](../c/calc_rangesel.md)
  - [btcostestimate](../b/btcostestimate.md)

## Notes and Other Information
- Returns true if a matching slot was found, false otherwise
- The extracted data is locally palloc'd and independent of the original tuple
- Caller must call `free_attstatsslot` to release allocated memory when done
- Supports searching by statistics kind and optionally by operator OID
- Can extract values only, numbers only, or both based on flags parameter
- Used extensively by selectivity estimation functions throughout the planner
- Handles both pass-by-value and pass-by-reference data types correctly
- Critical component of PostgreSQL's cost-based query optimization system
- Located in `src/backend/utils/cache/lsyscache.c:3234-3343`