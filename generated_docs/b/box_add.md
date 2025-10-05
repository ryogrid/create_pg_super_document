# box_add

## Location
[src/backend/utils/adt/geo_ops.c:4231-4245](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4231-L4245)

## Overview
The box_add function translates a BOX by adding a Point offset to both corners, effectively moving the entire box in 2D space.

## Definition
Datum box_add(PG_FUNCTION_ARGS)

## Detailed Description
This function performs vector addition between a BOX and a Point, translating the box by the specified offset. It takes a BOX and a Point as arguments and returns a new BOX where both the high and low corners have been translated by adding the Point's coordinates. The operation effectively moves the entire box in 2D space while preserving its size and shape. The function uses point_add_point to perform coordinate-wise addition on both corners of the box.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: Standard PostgreSQL function arguments containing:
  - Argument 0: BOX (box) - the box to be translated
  - Argument 1: Point (p) - the translation vector/offset

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOX_P (BOX argument extraction)
  - PG_GETARG_POINT_P (Point argument extraction)
  - [palloc](../p/palloc.md) (memory allocation)
  - [point_add_point](../p/point_add_point.md) (adds the point offset to each corner)
  - PG_RETURN_BOX_P (return value packaging)
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- The function allocates memory for a new BOX result using palloc
- Applies the same translation to both high and low corners of the box
- Preserves the box's dimensions and orientation
- Part of PostgreSQL's geometric data type arithmetic operations
- The operation is commutative: box + point = point + box
- Located in src/backend/utils/adt/geo_ops.c at lines 4231-4245

## Simplified Source

```c
Datum
box_add(PG_FUNCTION_ARGS)
{
    BOX *box = PG_GETARG_BOX_P(0);
    Point *p = PG_GETARG_POINT_P(1);
    BOX *result;

    // Allocate memory for result box
    result = (BOX *) palloc(sizeof(BOX));

    // Translate both corners by the point offset
    point_add_point(&result->high, &box->high, p);
    point_add_point(&result->low, &box->low, p);

    PG_RETURN_BOX_P(result);
}
```