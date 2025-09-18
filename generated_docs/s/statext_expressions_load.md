# statext_expressions_load

## Location
src/backend/statistics/extended_stats.c: 2405 - 2451

## Overview
Loads a specific pg_statistic record from stored expression statistics for a given statistics object and expression index.

## Definition


## Detailed Description
This function retrieves expression statistics that were previously stored for extended statistics objects. It looks up the statistics object by OID in the pg_statistic_ext_data system catalog, extracts the stxdexpr field (which contains serialized expression statistics), and returns the specific pg_statistic tuple for the requested expression index. The function uses PostgreSQL's expanded array infrastructure to efficiently access individual elements from the stored array of statistics tuples.

The function performs a cache lookup to find the statistics data, extracts the expression statistics array from the stxdexpr field, and then constructs a proper HeapTuple from the stored data at the specified index. This allows the query planner and other components to access expression statistics in the same format as regular column statistics.

## Parameters / Member Variables
- : OID of the extended statistics object containing the expression statistics
- : Boolean indicating whether to load inherited statistics (for partitioned tables)
- : Zero-based index of the expression within the statistics object

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache2](../S/SearchSysCache2.md), SysCacheGetAttr, DatumGetExpandedArray
  - deconstruct_expanded_array, DatumGetHeapTupleHeader, HeapTupleHeaderGetDatumLength
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md), heap_copytuple, ReleaseSysCache
- Called from (representative examples):
  - examine_variable

## Notes and Other Information
- Uses the STATEXTDATASTXOID system cache for efficient lookup of statistics data
- Handles the case where expression statistics haven't been built yet by throwing an error
- The returned HeapTuple is a copy that the caller is responsible for freeing
- Essential for query planning when expressions are used in WHERE clauses or other contexts requiring selectivity estimates
- Part of PostgreSQL's extended statistics infrastructure that supports multi-column and expression statistics