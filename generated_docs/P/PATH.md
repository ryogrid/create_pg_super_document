# PATH

## Location
[src/include/utils/geo_decls.h:122-132](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/geo_decls.h#L122-L132)

## Overview
PATH is a geometric data type in PostgreSQL that represents a sequence of connected line segments specified by vertex points, which can be either open or closed.

## Definition
```c
typedef struct
{
    int32   vl_len_;    /* varlena header (do not touch directly!) */
    int32   npts;
    int32   closed;     /* is this a closed polygon? */
    int32   dummy;      /* padding to make it double align */
    Point   p[FLEXIBLE_ARRAY_MEMBER];
} PATH;
```

## Detailed Description
PATH is a variable-length geometric data type that represents a series of connected points forming either an open path (sequence of line segments) or a closed path (polygon-like structure). It uses PostgreSQL's varlena structure to handle variable-length data efficiently. The path can contain any number of points, stored as a flexible array member.

A PATH can represent complex geometric shapes like polygonal chains, routes, boundaries, or any multi-segment linear feature. When the 'closed' flag is set, the path forms a closed loop by connecting the last point back to the first point, making it functionally similar to a polygon outline.

## Parameters / Member Variables
- `vl_len_`: PostgreSQL varlena header for variable-length data management (internal use)
- `npts`: Number of points in the path (int32)
- `closed`: Boolean flag indicating if the path is closed (1) or open (0)
- `dummy`: Padding field for double-alignment requirements
- `p[FLEXIBLE_ARRAY_MEMBER]`: Variable-length array of Point structures representing the vertices

## Dependencies
- Functions called/Symbols referenced:
  - [Point](Point.md) (vertex coordinates)
  - FLEXIBLE_ARRAY_MEMBER (variable-length array support)
  - int32 (PostgreSQL integer type)
  - [varlena](../v/varlena.md) header system

- Called from (representative examples):
  - [path_in](../p/path_in.md)/path_out (I/O functions)
  - [path_area](../p/path_area.md) (area calculation for closed paths)
  - [path_length](../p/path_length.md) (total length calculation)
  - [path_distance](../p/path_distance.md) (distance between paths)
  - [path_isclosed](../p/path_isclosed.md)/path_isopen (state queries)
  - [path_close](../p/path_close.md)/path_open (state modification)
  - [path_inter](../p/path_inter.md) (intersection tests)
  - [path_npoints](../p/path_npoints.md) (point count queries)
  - [path_poly](../p/path_poly.md) (conversion to polygon)
  - [poly_path](../p/poly_path.md) (conversion from polygon)

## Notes and Other Information
- Uses PostgreSQL's varlena system for efficient variable-length storage
- Can represent both open paths (polylines) and closed paths (polygon outlines)
- Memory layout is optimized with proper alignment via the dummy padding field
- Supports comprehensive geometric operations including area calculation, length measurement, and intersection tests
- Text representation typically follows the format '[(x1,y1),(x2,y2),...,(xn,yn)]' for open paths or '((x1,y1),(x2,y2),...,(xn,yn))' for closed paths
- When closed=1, the path behaves like a polygon outline with implicit connection from last to first point
- Extensively used in GIS applications for representing routes, boundaries, and complex linear features
- The flexible array member allows paths to contain any number of vertices limited only by PostgreSQL's maximum tuple size