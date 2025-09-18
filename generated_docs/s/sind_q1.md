# sind_q1

## Location
src/backend/utils/adt/float.c: 2272 - 2291

## Overview
The `sind_q1` function computes the sine of an angle in degrees for the first quadrant (0 to 90 degrees), providing a continuous monotonic function with exact results at key angles.

## Definition
```c
static double sind_q1(double x)
```

## Detailed Description
The `sind_q1` function is a static helper function that implements sine calculation for angles in the first quadrant (0 to 90 degrees). It uses a piecewise approach that stitches together two optimized functions to ensure continuity and accuracy:

- For angles from 0 to 30 degrees: Uses `sind_0_to_30(x)` directly
- For angles from 30 to 90 degrees: Uses the complementary angle identity sin(x) = cos(90° - x) via `cosd_0_to_60(90.0 - x)`

This design guarantees exact results at the boundary points (0°, 30°, and 90°) and maintains monotonicity across the entire range.

## Parameters / Member Variables
- `x`: The angle in degrees (expected to be in the range [0, 90])

## Dependencies
- Functions called/Symbols referenced:
  - [sind_0_to_30](sind_0_to_30.md)
  - [cosd_0_to_60](../c/cosd_0_to_60.md)
- Called from:
  - [init_degree_constants](../i/init_degree_constants.md)
  - [dcotd](../d/dcotd.md)
  - [dsind](../d/dsind.md)  
  - [dtand](../d/dtand.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same source file
- The function leverages trigonometric identities to maximize accuracy by using the most precise range of each helper function
- The piecewise design ensures continuity at x = 30° where the implementation switches from direct sine to complementary cosine calculation
- Used internally by PostgreSQL's degree-based trigonometric functions to provide high-precision results