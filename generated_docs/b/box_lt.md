# box_lt

## Location
[src/backend/utils/adt/geo_ops.c:744-752](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L744-L752)

## Overview
Compares two boxes by area and returns true if the area of the first box is less than the area of the second box.

## Definition
Datum box_lt(PG_FUNCTION_ARGS)

## Detailed Description
This function performs an area-based comparison between two boxes, determining if the area of box1 is strictly less than the area of box2. It is part of PostgreSQL's relational operators for the BOX data type. The comparison is done using floating-point arithmetic with appropriate precision handling through the FPlt function.

The function calculates the area of each box using the box_ar helper function and then compares these areas. This allows for size-based ordering and comparison of geometric box objects.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS convention:
  - Argument 0: BOX pointer (box1) - the first box whose area is being compared
  - Argument 1: BOX pointer (box2) - the second box for area comparison

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOX_P (macro to extract BOX argument from function args)
  - [box_ar](box_ar.md) (function to calculate box area)
  - [FPlt](../F/FPlt.md) (floating-point less than comparison)
  - PG_RETURN_BOOL (macro to return boolean result)
  - [BOX](../B/BOX.md) (box data structure)
- Called from (representative examples):
  - No current references found

## Notes and Other Information
- Part of the box relational operators family (box_relop)
- Performs area-based comparison rather than positional comparison
- Uses floating-point comparison to handle precision issues
- Returns true if area(box1) < area(box2)
- Comparison is done within PostgreSQL's accuracy constraints

## Simplified Source

```c
Datum box_lt(PG_FUNCTION_ARGS) {
    BOX *box1 = PG_GETARG_BOX_P(0);
    BOX *box2 = PG_GETARG_BOX_P(1);

    // Compare areas: return true if box1 area < box2 area
    PG_RETURN_BOOL(FPlt(box_ar(box1), box_ar(box2)));
}
```