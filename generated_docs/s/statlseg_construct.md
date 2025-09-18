# statlseg_construct

## Location
src/backend/utils/adt/geo_ops.c: 2142 - 2154

## Overview
A static inline helper function that constructs a line segment by copying coordinate values from two Point structures into a pre-allocated LSEG structure.

## Definition
```c
static inline void statlseg_construct(LSEG *lseg, Point *pt1, Point *pt2)
```

## Detailed Description
The `statlseg_construct` function is an internal utility function that initializes a line segment structure by copying coordinate data from two Point parameters. Unlike `lseg_construct`, this function assumes that memory for the LSEG has already been allocated by the caller. It performs a simple assignment of the x,y coordinates from each input point to the corresponding positions in the line segment's point array. This function is designed for efficiency and is used internally by other geometric functions that need to create temporary line segments as part of their calculations.

## Parameters / Member Variables
- `lseg`: LSEG pointer - pre-allocated line segment structure to be initialized
- `pt1`: Point pointer - the first endpoint of the line segment 
- `pt2`: Point pointer - the second endpoint of the line segment

## Dependencies
- Functions called/Symbols referenced:
  - Direct field access only - no function calls
- Data types used:
  - `[LSEG](../L/LSEG.md)` - line segment data type
  - `[Point](../P/Point.md)` - geometric point data type

## Notes and Other Information
- This is a static inline function, meaning it's not externally visible and is typically inlined at compile time
- The function assumes pre-allocated memory, making it efficient for internal use cases
- Widely used throughout the geometric operations codebase for creating temporary line segments
- Called by functions including: `lseg_construct`, `box_diagonal`, `path_inter`, `path_distance`, `box_closept_point`, `box_closept_lseg`, `box_interpt_lseg`, `inter_lb`, and `poly_distance`
- Essential building block for many geometric calculations involving line segments
- The naming follows PostgreSQL's convention where 'stat' prefix indicates static/internal functions