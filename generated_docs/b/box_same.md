# box_same

## Location
src/backend/utils/adt/geo_ops.c: 551 - 562

## Overview
Tests whether two BOX structures are identical by comparing their corner coordinates for exact equality.

## Definition


## Detailed Description
The `box_same` function determines if two BOX structures are geometrically identical by performing exact coordinate comparison. It checks both the high (upper-right) and low (lower-left) corner points of the boxes using point equality operations. This function provides exact equality testing, which is different from other BOX comparison operators that are based on area calculations.

The function is part of PostgreSQL's relational operator system for BOX types and is commonly used in indexing operations and query processing where exact matches are required.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention
  - Argument 0: BOX pointer - first box to compare
  - Argument 1: BOX pointer - second box to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOX_P (retrieves BOX arguments)
  - point_eq_point (point equality comparison function)
  - PG_RETURN_BOOL (returns boolean result)
  - BOX (box data structure)
- Called from (representative examples):
  - gist_box_leaf_consistent (GiST index consistency checking)
  - spg_box_quad_leaf_consistent (SP-GiST index consistency checking)

## Notes and Other Information
- Performs exact floating-point equality comparison, not approximate comparison
- Both corner points must match exactly for the boxes to be considered the same
- Part of the relational operators for BOX types, distinct from area-based comparisons (<, >, <=, >=)
- Frequently used in PostgreSQL's indexing systems (GiST and SP-GiST) for geometric data
- Returns boolean result suitable for SQL equality expressions (BOX '...' = BOX '...')
- Located in geo_ops.c alongside other geometric comparison functions