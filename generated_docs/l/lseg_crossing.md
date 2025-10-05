# lseg_crossing

## Location
[src/backend/utils/adt/geo_ops.c:5397-5456](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L5397-L5456)

## Overview
Determines if a line segment crosses the positive X-axis from the origin, used as a core component in ray-casting point-in-polygon algorithms.

## Definition

```c
static int
lseg_crossing(float8 x, float8 y, float8 prev_x, float8 prev_y)
```
## Detailed Description
The `lseg_crossing` function is a specialized geometric utility that determines whether a line segment defined by two points crosses the positive X-axis when viewed from the origin (0,0). This function is specifically designed to support ray-casting algorithms for point-in-polygon testing.

The function implements a complex decision tree that handles various edge cases including points on the axes, segments that contain the origin, and segments that cross axes in different directions. It returns different values based on the type and direction of crossing, allowing the caller to accumulate crossing counts to determine point containment.

The algorithm considers the segment from (prev_x, prev_y) to (x, y) and determines if this segment crosses the positive X-axis. Special handling is provided for degenerate cases where points lie exactly on the axes or where the segment passes through the origin.

## Parameters / Member Variables  
- `x` (float8): X-coordinate of the current point (endpoint of the segment)
- `y` (float8): Y-coordinate of the current point (endpoint of the segment)  
- `prev_x` (float8): X-coordinate of the previous point (startpoint of the segment)
- `prev_y` (float8): Y-coordinate of the previous point (startpoint of the segment)

## Dependencies
- Functions called/Symbols referenced:
  - FPzero, FPgt, FPlt, FPge, FPle (floating-point comparison macros)
  - [float8_mi](../f/float8_mi.md), float8_mul (floating-point arithmetic functions)
  - POINT_ON_POLYGON (constant for special case when segment contains origin)

- Called from (representative examples):
  - [point_inside](../p/point_inside.md) (uses this function to count axis crossings for point-in-polygon testing)

## Notes and Other Information
- This is a static (internal) function not directly exposed as a PostgreSQL function
- Return value meanings:
  - +2/-2: Segment crosses positive X-axis in positive/negative direction
  - +1/-1: One endpoint is on positive X-axis 
  - 0: No crossing or both points on positive X-axis
  - POINT_ON_POLYGON: Segment contains the origin (0,0)
- The function comment acknowledges the "confusing API" but notes it works correctly when results are summed for polygon containment testing
- Uses a determinant calculation (z = (x-prev_x)*y - (y-prev_y)*x) to determine which side of the line segment the origin lies on
- Handles floating-point precision issues through specialized comparison macros rather than direct equality testing  
- Critical component for robust point-in-polygon algorithms that must handle edge cases correctly
- The complex logic ensures proper handling of segments that are tangent to or pass through coordinate axes

## Simplified Source

```c
static int lseg_crossing(float8 x, float8 y, float8 prev_x, float8 prev_y) {
    if (FPzero(y)) {  // Current point on X-axis
        if (FPzero(x))  // Point is origin
            return POINT_ON_POLYGON;

        if (FPgt(x, 0)) {  // Point on positive X-axis
            if (FPzero(prev_y))  // Both points on X-axis
                return FPgt(prev_x, 0.0) ? 0 : POINT_ON_POLYGON;
            return FPlt(prev_y, 0.0) ? 1 : -1;  // Endpoint crossing
        } else {  // Point on negative X-axis
            if (FPzero(prev_y))
                return FPlt(prev_x, 0.0) ? 0 : POINT_ON_POLYGON;
            return 0;  // No positive X-axis involvement
        }
    }

    // Current point not on X-axis
    int y_sign = FPgt(y, 0.0) ? 1 : -1;

    if (FPzero(prev_y)) {  // Previous point on X-axis
        return FPlt(prev_x, 0.0) ? 0 : y_sign;
    }

    // Check if both points on same side of X-axis
    if ((y_sign < 0 && FPlt(prev_y, 0.0)) || (y_sign > 0 && FPgt(prev_y, 0.0)))
        return 0;  // No crossing

    // Segment crosses X-axis - check if it crosses positive part
    if (FPge(x, 0.0) && FPgt(prev_x, 0.0))
        return 2 * y_sign;  // Definite positive crossing

    if (FPlt(x, 0.0) && FPle(prev_x, 0.0))
        return 0;  // Crosses only negative X-axis

    // Mixed case - use determinant to check crossing point
    float8 z = (x - prev_x) * y - (y - prev_y) * x;
    if (FPzero(z))
        return POINT_ON_POLYGON;

    // Check crossing direction
    if ((y_sign < 0 && FPlt(z, 0.0)) || (y_sign > 0 && FPgt(z, 0.0)))
        return 0;

    return 2 * y_sign;
}
```