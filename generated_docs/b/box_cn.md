# box_cn

## Location
[src/backend/utils/adt/geo_ops.c:872-882](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L872-L882)

## Overview
Calculates and stores the center point coordinates of a geometric box into a provided Point structure.

## Definition


## Detailed Description
The `box_cn` function is a static helper function that computes the center point of a BOX geometric data type and stores the result in a provided Point structure. It calculates the center coordinates by taking the average of the high and low x-coordinates for the x-axis, and the average of the high and low y-coordinates for the y-axis. The function uses PostgreSQL's safe floating-point arithmetic functions to ensure proper handling of floating-point operations.

## Parameters / Member Variables
- `center`: Point pointer where the calculated center coordinates will be stored
- `box`: BOX pointer to the box geometry whose center point is to be calculated

## Dependencies
- Functions called/Symbols referenced:
  - [Point](../P/Point.md): PostgreSQL geometric point data type
  - [BOX](../B/BOX.md): PostgreSQL geometric box data type
  - [float8_pl](../f/float8_pl.md): PostgreSQL's safe floating-point addition function
  - [float8_div](../f/float8_div.md): PostgreSQL's safe floating-point division function
- Called from (representative examples):
  - [box_distance](box_distance.md): Function to calculate distance between box centers
  - [box_center](box_center.md): Function that returns the center point of a box
  - [box_interpt_lseg](box_interpt_lseg.md): Box-line segment intersection function
  - `PATH_CLOSED`: Path operations

## Notes and Other Information
- This function is a static helper located in `src/backend/utils/adt/geo_ops.c:872-882`
- The function modifies the provided Point structure in-place rather than returning a value
- Center calculation formula: center.x = (box.high.x + box.low.x) / 2, center.y = (box.high.y + box.low.y) / 2
- Uses PostgreSQL's safe arithmetic functions (`float8_pl`, `float8_div`) rather than direct C operators
- Widely used by other geometric functions that need box center coordinates
- Part of the internal implementation for PostgreSQL's geometric BOX data type operations