# box_gt

## Location
[src/backend/utils/adt/geo_ops.c:753-761](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L753-L761)

## Overview
Compares two boxes by area and returns true if the area of the first box is greater than the area of the second box.

## Definition
Datum box_gt(PG_FUNCTION_ARGS)

## Detailed Description
This function performs an area-based comparison between two boxes, determining if the area of box1 is strictly greater than the area of box2. It is the complement to box_lt and part of PostgreSQL's relational operators for the BOX data type. The comparison is done using floating-point arithmetic with appropriate precision handling through the FPgt function.

Like box_lt, this function calculates the area of each box using the box_ar helper function and then compares these areas. This enables size-based ordering and comparison of geometric box objects in descending order.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS convention:
  - Argument 0: BOX pointer (box1) - the first box whose area is being compared
  - Argument 1: BOX pointer (box2) - the second box for area comparison

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOX_P (macro to extract BOX argument from function args)
  - [box_ar](box_ar.md) (function to calculate box area)
  - [FPgt](../F/FPgt.md) (floating-point greater than comparison)
  - PG_RETURN_BOOL (macro to return boolean result)
  - [BOX](../B/BOX.md) (box data structure)
- Called from (representative examples):
  - No current references found

## Notes and Other Information
- Part of the box relational operators family (box_relop)
- Performs area-based comparison rather than positional comparison
- Uses floating-point comparison to handle precision issues
- Returns true if area(box1) > area(box2)
- Complement function to box_lt
- Comparison is done within PostgreSQL's accuracy constraints

## Simplified Source

```c
Datum box_gt(PG_FUNCTION_ARGS) {
    BOX *box1 = PG_GETARG_BOX_P(0);
    BOX *box2 = PG_GETARG_BOX_P(1);

    // Compare areas: return true if box1 area > box2 area
    PG_RETURN_BOOL(FPgt(box_ar(box1), box_ar(box2)));
}
```