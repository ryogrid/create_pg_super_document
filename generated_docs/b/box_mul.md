# box_mul

## Location
src/backend/utils/adt/geo_ops.c: 4261 - 4279

## Overview
Multiplies a box by a point, scaling the box dimensions by multiplying each corner coordinate with the corresponding point coordinate.

## Definition


## Detailed Description
The `box_mul` function implements geometric multiplication (scaling) between a box and a point. It creates a new box by multiplying the coordinates of both the high and low corner points of the input box with the corresponding coordinates of the input point. This operation scales the box dimensions along each axis. The function uses temporary Point variables to store the multiplication results and then constructs a properly oriented box using `box_construct`, which ensures the high and low corners are correctly assigned regardless of the multiplication results.

## Parameters / Member Variables
- `box`: Input box (first argument) to be multiplied/scaled
- `p`: Input point (second argument) containing scaling factors for x and y coordinates
- `result`: Newly allocated box containing the scaled result
- `high`: Temporary point to store the result of multiplying box->high with p
- `low`: Temporary point to store the result of multiplying box->low with p

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOX_P (macro to extract box argument)
  - PG_GETARG_POINT_P (macro to extract point argument)
  - [palloc](../p/palloc.md) (memory allocation)
  - [point_mul_point](../p/point_mul_point.md) (point multiplication helper)
  - [box_construct](box_construct.md) (constructs a properly oriented box from two points)
  - PG_RETURN_BOX_P (macro to return box result)
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's geometric data type operations
- Uses `box_construct` to ensure the resulting box has properly ordered high and low corners, as multiplication can potentially reverse the corner relationships
- The scaling is performed independently on x and y coordinates, allowing for non-uniform scaling
- Memory for the result is allocated using palloc and will be managed by PostgreSQL's memory context system
- Negative scaling factors in the point will flip the box orientation, which is handled correctly by `box_construct`