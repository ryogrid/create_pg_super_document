# box_sub

## Location
src/backend/utils/adt/geo_ops.c: 4246 - 4260

## Overview
Subtracts a point from a box by translating both the high and low corners of the box by the negative of the point coordinates.

## Definition


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
  - palloc (memory allocation)
  - point_sub_point (point subtraction helper)
  - PG_RETURN_BOX_P (macro to return box result)
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's geometric data type operations
- Returns a new box rather than modifying the input box in place
- The subtraction is performed on both corner points to maintain the box's shape while translating its position
- Memory for the result is allocated using palloc and will be managed by PostgreSQL's memory context system