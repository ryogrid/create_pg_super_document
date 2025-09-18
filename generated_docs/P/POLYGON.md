# POLYGON

## Location
src/include/utils/geo_decls.h: 157 - 165

## Overview
POLYGON is a geometric data type in PostgreSQL that represents a closed polygonal shape specified by an array of vertex points, with an embedded bounding box for performance optimization.

## Definition
```c
typedef struct
{
    int32   vl_len_;     /* varlena header (do not touch directly!) */
    int32   npts;
    BOX     boundbox;
    Point   p[FLEXIBLE_ARRAY_MEMBER];
} POLYGON;
```

## Detailed Description
POLYGON represents a closed polygonal shape in 2D space defined by a sequence of vertex points that form the polygon boundary. It uses PostgreSQL's varlena structure for variable-length storage and includes several performance optimizations: a cached bounding box (BOX) for quick spatial filtering, and a point count field for efficient operations.

The polygon is always considered closed, with an implicit edge connecting the last vertex back to the first vertex. This structure is optimized for spatial queries and geometric operations, with the precomputed bounding box enabling fast preliminary spatial filtering before more expensive point-in-polygon tests.

## Parameters / Member Variables
- `vl_len_`: PostgreSQL varlena header for variable-length data management (internal use)
- `npts`: Number of vertex points in the polygon (int32)
- `boundbox`: Precomputed bounding box (BOX) for performance optimization
- `p[FLEXIBLE_ARRAY_MEMBER]`: Variable-length array of Point structures representing polygon vertices

## Dependencies
- Functions called/Symbols referenced:
  - BOX (bounding box storage)
  - Point (vertex coordinates)
  - FLEXIBLE_ARRAY_MEMBER (variable-length array support)
  - int32 (PostgreSQL integer type)
  - varlena header system

- Called from (representative examples):
  - poly_in/poly_out (I/O functions)
  - poly_contain_pt/pt_contained_poly (point containment tests)
  - poly_overlap/poly_contain (polygon relationship tests)
  - poly_distance (distance calculations)
  - poly_center (centroid calculation)
  - poly_box (bounding box extraction)
  - poly_path/path_poly (conversion with PATH type)
  - poly_circle/circle_poly (conversion with CIRCLE type)
  - GiST spatial indexing operations
  - SP-GiST spatial partitioning

## Notes and Other Information
- Uses PostgreSQL's varlena system for efficient variable-length storage of polygon data
- The polygon is always considered closed with implicit connection from last to first vertex
- Includes precomputed bounding box for performance optimization in spatial queries
- The bounding box enables fast spatial filtering before expensive geometric operations
- Supports comprehensive geometric operations including area calculation, containment tests, and spatial relationships
- Text representation typically follows the format '((x1,y1),(x2,y2),...,(xn,yn))'
- Extensively used in GIS applications for representing geographic boundaries, land parcels, and complex shapes
- The cached bounding box makes POLYGON particularly efficient for spatial indexing in GiST indexes
- Memory layout is optimized with proper alignment and variable-length vertex storage
- Can be converted to and from other geometric types like PATH, BOX, and CIRCLE