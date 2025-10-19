# box_above_eq

## Location
[src/backend/utils/adt/geo_ops.c:731-743](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L731-L743)

## Overview
Determines if the first box is entirely above or at the same level as the second box.

## Definition
Datum box_above_eq(PG_FUNCTION_ARGS)

## Detailed Description
This function checks if box1 is positioned entirely above box2 or shares the same boundary level. It compares the lowest Y coordinate of box1 with the highest Y coordinate of box2. Like box_below_eq, this function is part of PostgreSQL's geometric data type operations but is marked as deprecated and obsolete.

This function is the counterpart to box_below_eq and suffers from the same issues - it probably erroneously accepts the equal-boundaries case and is not in sync with other positional operators. It was deprecated and not supported in the PostgreSQL 8.1 rtree operator class extension.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS convention:
  - Argument 0: BOX pointer (box1) - the box being tested for above position
  - Argument 1: BOX pointer (box2) - the reference box for comparison

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOX_P (macro to extract BOX argument from function args)
  - [FPge](../F/FPge.md) (floating-point greater than or equal comparison)
  - PG_RETURN_BOOL (macro to return boolean result)
  - [BOX](../B/BOX.md) (box data structure)
- Called from (representative examples):
  - No current references found (function is deprecated)

## Notes and Other Information
- This function is deprecated and obsolete
- Not supported in PostgreSQL 8.1+ rtree operator class extension
- Probably erroneously accepts equal-boundaries case
- Part of the box positional operators family
- Returns true if box1.low.y >= box2.high.y
- Complement function to box_below_eq

## Simplified Source

```c
Datum box_above_eq(PG_FUNCTION_ARGS) {
    BOX *box1 = PG_GETARG_BOX_P(0);
    BOX *box2 = PG_GETARG_BOX_P(1);

    // Check if box1 is entirely above or touching box2
    // This is deprecated and accepts equal boundaries (possibly erroneously)
    PG_RETURN_BOOL(box1->low.y >= box2->high.y);
}
```