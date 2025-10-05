# box_contain_point

## Location
[src/backend/utils/adt/geo_ops.c:3130-3136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L3130-L3136)

## Overview
Determines whether a given point lies within a box (rectangle) or on its border using simple coordinate range checking.

## Definition
```c
static bool box_contain_point(BOX *box, Point *point)
```

## Detailed Description
This static utility function implements geometric containment testing for points within rectangular boxes (axis-aligned rectangles). The function performs a straightforward containment test by checking if the point's coordinates fall within the box's coordinate ranges in both x and y dimensions.

The algorithm checks four conditions: the point's x-coordinate must be between the box's low.x and high.x values (inclusive), and the point's y-coordinate must be between the box's low.y and high.y values (inclusive). The function explicitly includes points that lie exactly on the box border, making this an inclusive containment test.

This is one of the simplest geometric containment algorithms, requiring only four floating-point comparisons, making it highly efficient for bounding box operations commonly used in spatial indexing and geometric filtering.

## Parameters / Member Variables
- `box`: Pointer to a BOX structure containing the rectangular bounds defined by low and high corner points
- `point`: Pointer to a Point structure containing the x and y coordinates to test for containment

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only direct member access and comparison operators)
- Data types used:
  - [BOX](../B/BOX.md) (rectangular box representation with low and high corner points)
  - [Point](../P/Point.md) (point representation with x, y coordinates)
- Called from (representative examples):
  - [on_pb](../o/on_pb.md) (point on box test)
  - [box_closept_point](box_closept_point.md) (closest point calculations)
  - [box_contain_pt](box_contain_pt.md) (public box containment interface)
  - [box_contain_lseg](box_contain_lseg.md) (box-line segment containment)
  - [box_interpt_lseg](box_interpt_lseg.md) (box-line segment intersection testing)

## Notes and Other Information
- This is a static function, accessible only within geo_ops.c
- Uses inclusive boundary testing - points exactly on the box border are considered contained
- Extremely efficient with only four floating-point comparisons
- Assumes the BOX structure maintains the invariant that low.x ≤ high.x and low.y ≤ high.y
- Fundamental building block for more complex geometric operations involving rectangular regions
- Often used as a preliminary filter in spatial operations before more expensive geometric tests
- Part of PostgreSQL's geometric operations infrastructure, particularly important for bounding box operations
- The inclusive boundary behavior is important for spatial queries and geometric consistency

## Simplified Source

```c
static bool box_contain_point(BOX *box, Point *point) {
    // Check if point coordinates are within box bounds (inclusive)
    return box->high.x >= point->x && box->low.x <= point->x &&
           box->high.y >= point->y && box->low.y <= point->y;
}
```