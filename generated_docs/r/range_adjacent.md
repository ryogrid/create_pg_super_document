# range_adjacent

## Location
src/backend/utils/adt/rangetypes.c: 828 - 840

## Overview
PostgreSQL SQL function that determines if two ranges are adjacent (touching but not overlapping).

## Definition


## Detailed Description
This function serves as the SQL-callable wrapper for the range adjacency operator "-|-". It extracts two range arguments from the PostgreSQL function call context, validates they are of the same type, and delegates the actual adjacency testing to .

Two ranges are considered adjacent if they touch at exactly one boundary point without overlapping or having a gap between them. This is useful for range operations where you need to detect ranges that can be merged or are positioned next to each other.

## Parameters / Member Variables
- Uses  macro to access function arguments:
  -  (argument 0): First RangeType to test
  -  (argument 1): Second RangeType to test

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_RANGE_P
  - [range_get_typcache](range_get_typcache.md)
  - RangeTypeGetOid
  - [range_adjacent_internal](range_adjacent_internal.md)
- Called from (representative examples):
  - No direct callers found (SQL operator function)

## Notes and Other Information
- This function implements the PostgreSQL "-|-" (adjacent) range operator
- Returns a boolean datum indicating whether the ranges are adjacent
- The function is registered in the PostgreSQL system catalogs as the implementation for range adjacency operations
- Relies on range_adjacent_internal for the core adjacency logic
- Located in src/backend/utils/adt/rangetypes.c:828-840