# box_div

## Location
src/backend/utils/adt/geo_ops.c: 4280 - 4301

## Overview
Divides a box by a point, scaling down the box dimensions by dividing each corner coordinate by the corresponding point coordinate.

## Definition
```c
Datum box_div(PG_FUNCTION_ARGS)
```

## Detailed Description
The `box_div` function implements geometric division (inverse scaling) between a box and a point. It creates a new box by dividing the coordinates of both the high and low corner points of the input box by the corresponding coordinates of the input point. This operation scales down the box dimensions along each axis by the reciprocal of the point coordinates. The function uses temporary Point variables to store the division results and then constructs a properly oriented box using `box_construct`, which ensures the high and low corners are correctly assigned regardless of the division results.

## Parameters / Member Variables
- `box`: Input box (first argument) to be divided/scaled down
- `p`: Input point (second argument) containing divisors for x and y coordinates
- `result`: Newly allocated box containing the scaled result
- `high`: Temporary point to store the result of dividing box->high by p
- `low`: Temporary point to store the result of dividing box->low by p

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOX_P (macro to extract box argument)
  - PG_GETARG_POINT_P (macro to extract point argument)
  - [palloc](../p/palloc.md) (memory allocation)
  - [point_div_point](../p/point_div_point.md) (point division helper)
  - [box_construct](box_construct.md) (constructs a properly oriented box from two points)
  - PG_RETURN_BOX_P (macro to return box result)
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's geometric data type operations
- Uses `box_construct` to ensure the resulting box has properly ordered high and low corners, as division can potentially affect corner relationships
- The division is performed independently on x and y coordinates, allowing for non-uniform scaling
- Memory for the result is allocated using palloc and will be managed by PostgreSQL's memory context system
- Division by zero will be handled by the underlying `point_div_point` function, which may raise an error or produce infinite/NaN values depending on PostgreSQL's floating-point handling
- Negative divisors in the point will flip the box orientation, which is handled correctly by `box_construct`