# cosd_q1

## Location
[src/backend/utils/adt/float.c:2292-2310](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L2292-L2310)

## Overview
The `cosd_q1` function computes the cosine of an angle in degrees for the first quadrant (0 to 90 degrees), providing a continuous monotonic function with exact results at key angles.

## Definition
```c
static double cosd_q1(double x)
```

## Detailed Description
The `cosd_q1` function is a static helper function that implements cosine calculation for angles in the first quadrant (0 to 90 degrees). It uses a piecewise approach that stitches together two optimized functions to ensure continuity and accuracy:

- For angles from 0 to 60 degrees: Uses `cosd_0_to_60(x)` directly  
- For angles from 60 to 90 degrees: Uses the complementary angle identity cos(x) = sin(90° - x) via `sind_0_to_30(90.0 - x)`

This design guarantees exact results at the boundary points (0°, 60°, and 90°) and maintains monotonicity across the entire range.

## Parameters / Member Variables
- `x`: The angle in degrees (expected to be in the range [0, 90])

## Dependencies
- Functions called/Symbols referenced:
  - [cosd_0_to_60](cosd_0_to_60.md)
  - [sind_0_to_30](../s/sind_0_to_30.md)
- Called from:
  - [init_degree_constants](../i/init_degree_constants.md)
  - [dcosd](../d/dcosd.md)
  - [dcotd](../d/dcotd.md)
  - [dtand](../d/dtand.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same source file
- The function leverages trigonometric identities to maximize accuracy by using the most precise range of each helper function
- The piecewise design ensures continuity at x = 60° where the implementation switches from direct cosine to complementary sine calculation
- Used internally by PostgreSQL's degree-based trigonometric functions to provide high-precision results
- Complements the `sind_q1` function, together providing complete first quadrant trigonometric coverage

## Simplified Source

```c
static double cosd_q1(double x) {
    // Calculate cosine for first quadrant angles (0-90 degrees)
    // Uses optimal range for each helper function

    if (x <= 60.0) {
        return cosd_0_to_60(x);
    } else {
        // Use complementary angle: cos(x) = sin(90° - x)
        return sind_0_to_30(90.0 - x);
    }
}
```