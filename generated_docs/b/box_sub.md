# box_sub

## Location
[src/backend/utils/adt/geo_ops.c:4246-4260](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4246-L4260)

## Overview
Subtracts a point from a box by translating both the high and low corners of the box by the negative of the point coordinates.

## Definition

```c
struct(result, &high, &low);
```
## Detailed Description
The  function implements geometric subtraction between a box and a point. It creates a new box by subtracting the point coordinates from both the high and low corner points of the input box. This operation effectively translates the entire box in the opposite direction of the point vector. The function allocates memory for the result box and uses the  helper function to perform the coordinate-wise subtraction on each corner.

## Parameters / Member Variables
- : Input box (first argument) from which the point will be subtracted
- : Input point (second argument) to subtract from the box
- : Newly allocated box containing the result of the subtraction

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOX_P (macro to extract box argument)
  - PG_GETARG_POINT_P (macro to extract point argument)
  - [palloc](../p/palloc.md) (memory allocation)
  - [point_sub_point](../p/point_sub_point.md) (point subtraction helper)
  - PG_RETURN_BOX_P (macro to return box result)
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's geometric data type operations
- Returns a new box rather than modifying the input box in place
- The subtraction is performed on both corner points to maintain the box's shape while translating its position
- Memory for the result is allocated using palloc and will be managed by PostgreSQL's memory context system

## Simplified Source

```c
Datum
box_sub(PG_FUNCTION_ARGS)
{
    BOX *box = PG_GETARG_BOX_P(0);
    Point *p = PG_GETARG_POINT_P(1);
    BOX *result;

    // Allocate memory for result box
    result = (BOX *) palloc(sizeof(BOX));

    // Subtract point from both corners to translate the box
    point_sub_point(&result->high, &box->high, p);
    point_sub_point(&result->low, &box->low, p);

    PG_RETURN_BOX_P(result);
}
```