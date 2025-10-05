# lseg_interpt_line

## Location
[src/backend/utils/adt/geo_ops.c:2675-2723](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2675-L2723)

## Overview
A static function that determines if a line segment intersects with a line and optionally returns the intersection point.

## Definition

```c
struct(&tmp, &lseg->p[0], lseg_sl(lseg));
```
## Detailed Description
This function calculates whether a line segment (LSEG) intersects with an infinite line (LINE) and can optionally return the intersection point. The algorithm works in two phases: first, it promotes the line segment to an infinite line and finds the intersection point between the two lines using . If an intersection exists, it then verifies that this intersection point actually lies within the bounds of the original line segment using . If both conditions are met, the function returns true and optionally sets the result point. The function includes special handling for endpoint matches to avoid floating-point precision issues.

## Parameters / Member Variables
- : Pointer to Point where intersection point will be stored (can be NULL if only boolean result is needed)
- : Pointer to the line segment (LSEG) to test for intersection
- : Pointer to the infinite line (LINE) to test for intersection

## Dependencies
- Functions called/Symbols referenced:
  -  - Creates a line from a point and slope
  -  - Calculates slope of line segment
  -  - Finds intersection point of two infinite lines
  -  - Checks if point lies on line segment
  -  - Tests equality of two points
  -  - [Point](../P/Point.md) data structure
  -  - Line segment data structure
  -  - Infinite line data structure
- Called from (representative examples):
  -  - Line segment to line segment intersection
  -  - Closest point on line segment to line
  -  - Intersection of line segment and line
  -  - Intersection of line and box

## Notes and Other Information
- This is a static internal function, not directly accessible from SQL
- Returns boolean result indicating whether intersection exists
- Handles floating-point precision issues by explicitly checking for endpoint matches
- Uses a two-phase algorithm: line-line intersection followed by containment verification
- The result parameter can be NULL if only the boolean intersection result is needed
- Part of PostgreSQL's geometric intersection calculation system

## Simplified Source

```c
static bool
lseg_interpt_line(Point *result, LSEG *lseg, LINE *line)
{
    Point interpt;
    LINE tmp;

    // Convert line segment to infinite line for intersection calculation
    line_construct(&tmp, &lseg->p[0], lseg_sl(lseg));

    // Find intersection between the two infinite lines
    if (!line_interpt_line(&interpt, &tmp, line))
        return false;

    // Check if intersection point lies within the line segment bounds
    if (!lseg_contain_point(lseg, &interpt))
        return false;

    // If result requested, handle floating-point precision by checking endpoints
    if (result != NULL)
    {
        if (point_eq_point(&lseg->p[0], &interpt))
            *result = lseg->p[0];
        else if (point_eq_point(&lseg->p[1], &interpt))
            *result = lseg->p[1];
        else
            *result = interpt;
    }

    return true;
}
```