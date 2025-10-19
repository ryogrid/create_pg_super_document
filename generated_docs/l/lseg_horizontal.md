# lseg_horizontal

## Location
[src/backend/utils/adt/geo_ops.c:2227-2235](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2227-L2235)

## Overview
Determines if a line segment is horizontal by checking if both endpoints have the same y-coordinate.

## Definition

```c
Datum
lseg_horizontal(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function tests whether a line segment is horizontal. A line segment is considered horizontal if both of its endpoints have the same y-coordinate, meaning the segment runs parallel to the x-axis. The function compares the y-coordinates of the two points that define the line segment using floating-point equality comparison.

## Parameters / Member Variables
- : Line segment (LSEG type) obtained via 
  - : y-coordinate of the first point
  - : y-coordinate of the second point

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract LSEG argument from function call
  - : Floating-point equality comparison function
  - : Macro to return boolean result
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Returns a boolean Datum indicating whether the segment is horizontal
- Uses floating-point comparison to handle precision issues when comparing coordinates
- Part of PostgreSQL's geometric data type operations for line segments
- A horizontal line segment has a slope of zero
- Located in geo_ops.c alongside other geometric utility functions
- Complementary to function

## Simplified Source

```c
Datum lseg_horizontal(PG_FUNCTION_ARGS) {
    // Get the line segment from function arguments
    LSEG *lseg = PG_GETARG_LSEG_P(0);

    // Check if both endpoints have same y-coordinate (horizontal line)
    PG_RETURN_BOOL(FPeq(lseg->p[0].y, lseg->p[1].y));
}
```