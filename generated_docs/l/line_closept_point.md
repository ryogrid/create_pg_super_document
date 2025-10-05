# line_closept_point

## Location
[src/backend/utils/adt/geo_ops.c:2724-2749](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2724-L2749)

## Overview
A static function that finds the closest point on an infinite line to a given point and returns the distance between them.

## Definition

```c
struct(&tmp, point, line_invsl(line));
```
## Detailed Description
This function calculates the shortest distance from a point to an infinite line by finding the intersection point of a perpendicular line dropped from the point to the target line. It constructs a perpendicular line passing through the given point using the inverse slope of the target line, then finds where this perpendicular intersects the original line. This intersection point represents the closest point on the line to the given point. The function handles edge cases like NaN coordinates or roundoff issues by returning NaN when the intersection cannot be computed. If successful, it returns the Euclidean distance between the original point and the closest point on the line.

## Parameters / Member Variables
- : Pointer to Point where the closest point on the line will be stored (can be NULL if only distance is needed)
- : Pointer to the infinite line (LINE) to find closest point on
- : Pointer to the point from which to measure distance

## Dependencies
- Functions called/Symbols referenced:
  -  - Creates a line from a point and slope
  -  - Calculates inverse slope of a line (perpendicular slope)
  -  - Finds intersection point of two infinite lines
  -  - Returns NaN float8 value for error cases
  -  - Calculates Euclidean distance between two points
  -  - [Point](../P/Point.md) data structure
  -  - Infinite line data structure
- Called from (representative examples):
  -  - Distance from point to line
  -  - Distance from line to point
  -  - Closest point from point to line
  -  - Closest point on line segment to line

## Notes and Other Information
- This is a static internal function, not directly accessible from SQL
- Uses geometric principle of perpendicular distance being the shortest distance
- Handles numerical edge cases by returning NaN when intersection fails
- The result parameter can be NULL if only the distance value is needed
- Returns both the closest point (via result parameter) and the distance (as return value)
- Fundamental building block for point-to-line distance calculations in PostgreSQL geometric operations

## Simplified Source

```c
static float8
line_closept_point(Point *result, LINE *line, Point *point)
{
    Point closept;
    LINE tmp;

    // Create perpendicular line through the point using inverse slope
    line_construct(&tmp, point, line_invsl(line));

    // Find intersection of perpendicular with original line (closest point)
    if (!line_interpt_line(&closept, &tmp, line))
    {
        // Handle edge cases (NaN coordinates, roundoff issues)
        if (result != NULL)
            *result = *point;
        return get_float8_nan();
    }

    // Store closest point if requested
    if (result != NULL)
        *result = closept;

    // Return distance between original point and closest point on line
    return point_dt(&closept, point);
}
```