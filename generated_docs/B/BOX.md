# BOX

## Location
src/include/utils/geo_decls.h: 144 - 156

## Overview
BOX is a geometric data type in PostgreSQL that represents a rectangular bounding box specified by two corner points, which are automatically sorted for efficient geometric operations.

## Definition
```c
typedef struct
{
    Point   high,
            low;    /* corner POINTs */
} BOX;
```

## Detailed Description
BOX represents an axis-aligned rectangular region in 2D space defined by two corner points: 'high' and 'low'. PostgreSQL automatically sorts these points so that 'high' contains the upper-right corner coordinates (maximum x and y values) and 'low' contains the lower-left corner coordinates (minimum x and y values). This consistent ordering optimization eliminates the need for coordinate sorting during geometric calculations.

BOX is fundamental to PostgreSQL's spatial indexing systems, particularly GiST (Generalized Search Tree) indexes, where it serves as bounding rectangles for complex geometric objects. It's extensively used in spatial queries for containment tests, overlap detection, and distance calculations.

## Parameters / Member Variables
- `high`: Point structure containing the upper-right corner coordinates (maximum x,y values)
- `low`: Point structure containing the lower-left corner coordinates (minimum x,y values)

## Dependencies
- Functions called/Symbols referenced:
  - Point (corner coordinate storage)
  
- Called from (representative examples):
  - POLYGON (bounding box storage)
  - GiST index operations (spatial indexing)
  - box_in/box_out (I/O functions)
  - box_overlap/box_contain (geometric relationship tests)
  - box_area/box_width/box_height (measurement functions)
  - box_distance (distance calculations)
  - box_intersect (intersection operations)
  - Circle and Polygon operations (bounding box calculations)
  - SP-GiST spatial partitioning algorithms

## Notes and Other Information
- Corner points are automatically sorted with high containing max coordinates and low containing min coordinates
- This consistent sorting eliminates redundant coordinate comparisons in geometric operations
- Extensively used in PostgreSQL's spatial indexing infrastructure, particularly GiST indexes
- Supports comprehensive geometric operations including area, width, height, containment, overlap, and intersection tests
- Text representation typically follows the format '(x1,y1),(x2,y2)' where coordinates are automatically ordered
- Fundamental building block for bounding box calculations in more complex geometric types like POLYGON and CIRCLE
- Optimized for high-performance spatial queries and range searches
- The sorted coordinate guarantee makes BOX ideal for spatial partitioning algorithms in SP-GiST indexes