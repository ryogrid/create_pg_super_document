# LSEG

## Location
src/include/utils/geo_decls.h: 109 - 121

## Overview
LSEG represents a line segment in PostgreSQL's geometric type system, specified by two endpoints as Point structures.

## Definition
```c
typedef struct
{
    Point   p[2];
} LSEG;
```

## Detailed Description
LSEG (Line Segment) is a geometric data type that represents a finite line segment defined by exactly two endpoints. Each endpoint is stored as a Point structure containing x,y coordinates. Unlike the infinite LINE type, LSEG has definite start and end points, making it suitable for representing bounded linear geometric objects.

The LSEG type is widely used in PostgreSQL's geometric operations for distance calculations, intersection tests, containment checks, and spatial indexing. It serves as a building block for more complex geometric types and supports a comprehensive set of geometric operators and functions.

## Parameters / Member Variables
- `p[2]`: An array of two Point structures representing the line segment endpoints
  - `p[0]`: First endpoint of the line segment
  - `p[1]`: Second endpoint of the line segment

## Dependencies
- Functions called/Symbols referenced:
  - Point (endpoint coordinates)
  - FLEXIBLE_ARRAY_MEMBER (for PATH structure compatibility)

- Called from (representative examples):
  - lseg_in/lseg_out (I/O functions)
  - lseg_distance (distance calculations)
  - lseg_intersect (intersection tests)
  - lseg_parallel/lseg_perp (geometric relationship tests)
  - lseg_construct (constructor function)
  - PATH operations (as line segments within paths)
  - BOX operations (diagonal calculations)
  - POLYGON operations (edge representations)

## Notes and Other Information
- LSEG represents a finite line segment, distinct from the infinite LINE type
- The order of endpoints p[0] and p[1] can be significant for certain operations
- Supports comprehensive geometric operations including length, slope, intersection, containment, and distance calculations
- Used extensively in spatial indexing and geometric query processing
- Text representation typically follows the format '[(x1,y1),(x2,y2)]'
- The type integrates with PostgreSQL's operator system for geometric comparisons and transformations
- Commonly used in GIS applications and spatial data processing within PostgreSQL