# serialize_expr_stats

## Location
[src/backend/statistics/extended_stats.c:2275-2404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L2275-L2404)

## Overview
Serializes expression statistics into an array of pg_statistic rows, converting VacAttrStats data for expressions into a format suitable for storage in PostgreSQL's statistics system.

## Definition


## Detailed Description
This function takes expression analysis data and creates an array of pg_statistic tuples that represent the statistical information gathered about expressions during ANALYZE operations. For each expression in the input array, it constructs a complete pg_statistic row with all the standard statistical fields (null fraction, width, distinct values, most common values, histograms, etc.). The function handles both valid statistics (where stats_valid is true) and invalid ones by either creating a proper tuple or adding a null entry to maintain array consistency.

The function opens the pg_statistic system catalog to get the composite type information, then iterates through each expression's statistics data. For each expression, it builds a HeapTuple containing all the pg_statistic fields, using the VacAttrStats structure to populate stakind, staop, stacoll, stanumbers, and stavalues arrays. The resulting tuples are accumulated into an array using PostgreSQL's array building infrastructure.

## Parameters / Member Variables
- : Array of AnlExprData structures containing expression analysis results and associated VacAttrStats
- : Number of expressions in the exprdata array

## Dependencies
- Functions called/Symbols referenced:
  - table_open, get_rel_type_id, accumArrayResult, heap_form_tuple
  - [construct_array_builtin](../c/construct_array_builtin.md), construct_array, heap_copy_tuple_as_datum
  - [makeArrayResult](../m/makeArrayResult.md), ObjectIdGetDatum, Int16GetDatum, Float4GetDatum
- Called from (representative examples):
  - [BuildRelationExtStatistics](../B/BuildRelationExtStatistics.md)

## Notes and Other Information
- Uses InvalidOid and InvalidAttrNumber for starelid and staattnum since these are expression statistics, not column statistics
- Handles both numeric statistics arrays (stanumbers) and value statistics arrays (stavalues) 
- Memory allocation is done in CurrentMemoryContext
- The function maintains the same structure as regular attribute statistics but marks them as expression-based
- Essential for extended statistics functionality that includes expressions beyond simple column references