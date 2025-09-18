# box_below_eq

## Location
src/backend/utils/adt/geo_ops.c: 722 - 730

## Overview
Determines if the first box is entirely below or at the same level as the second box.

## Definition
Datum box_below_eq(PG_FUNCTION_ARGS)

## Detailed Description
This function checks if box1 is positioned entirely below box2 or shares the same boundary level. It compares the highest Y coordinate of box1 with the lowest Y coordinate of box2. The function is part of PostgreSQL's geometric data type operations but is marked as deprecated and obsolete.

According to the source comments, this function probably erroneously accepts the equal-boundaries case and is not in sync with other positional operators like box_left and box_right. It was deprecated and not supported in the PostgreSQL 8.1 rtree operator class extension.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS convention:
  - Argument 0: BOX pointer (box1) - the box being tested for below position
  - Argument 1: BOX pointer (box2) - the reference box for comparison

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOX_P (macro to extract BOX argument from function args)
  - FPle (floating-point less than or equal comparison)
  - PG_RETURN_BOOL (macro to return boolean result)
  - BOX (box data structure)
- Called from (representative examples):
  - No current references found (function is deprecated)

## Notes and Other Information
- This function is deprecated and obsolete
- Not supported in PostgreSQL 8.1+ rtree operator class extension
- Probably erroneously accepts equal-boundaries case
- Part of the box positional operators family
- Returns true if box1.high.y <= box2.low.y