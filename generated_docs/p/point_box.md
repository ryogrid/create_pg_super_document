# point_box

## Location
src/backend/utils/adt/geo_ops.c: 4302 - 4320

## Overview
Converts a point to an empty box (degenerate box) where both the high and low corners are set to the same point coordinates.

## Definition
```c
Datum point_box(PG_FUNCTION_ARGS)
```

## Detailed Description
The `point_box` function converts a single point into a box representation by creating a degenerate (zero-area) box where both the high and low corner points have identical coordinates matching the input point. This type of box represents a single point in box form and has zero area. The function allocates memory for a new box and explicitly sets both x and y coordinates of both corners to match the input point's coordinates.

## Parameters / Member Variables
- `pt`: Input point (first argument) to be converted to box format
- `box`: Newly allocated box structure to hold the converted point

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINT_P (macro to extract point argument)
  - [palloc](palloc.md) (memory allocation)
  - PG_RETURN_BOX_P (macro to return box result)
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's geometric data type operations and serves as a type conversion utility
- Creates a degenerate box with zero area, which is useful for geometric operations that need to treat points as boxes
- The resulting box has high.x = low.x = pt->x and high.y = low.y = pt->y
- Memory for the result is allocated using palloc and will be managed by PostgreSQL's memory context system
- This conversion allows points to participate in box-oriented geometric operations and comparisons
- The comment explicitly mentions this creates an "empty box", referring to the zero-area nature of the result