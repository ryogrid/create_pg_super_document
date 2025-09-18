# gist_point_compress

## Location
src/backend/access/gist/gistproc.c: 1168 - 1195

## Overview
Implements the GiST compress method for points, converting leaf-level point data into bounding box format for consistent internal representation in the GiST index tree.

## Definition
```c
Datum gist_point_compress(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the compress method for point data types in GiST indexes. The compress method is responsible for converting the original data format into a representation suitable for storage and operations within the index tree. For points, this involves creating a degenerate bounding box where both the high and low coordinates are set to the point's coordinates.

The function distinguishes between leaf and internal nodes: for leaf nodes (where leafkey is true), it creates a new BOX structure representing the point as a zero-area rectangle. For internal nodes, the entry is already in the proper format and is returned unchanged. This design allows the index to maintain a uniform bounding box representation throughout the tree while preserving the original point data at the leaf level.

## Parameters / Member Variables
- `entry`: GiST entry pointer containing either a point (at leaf level) or a bounding box (at internal levels)

## Dependencies
- Functions called/Symbols referenced:
  - `[palloc](../p/palloc.md)`: Memory allocation for creating new box and entry structures
  - `[DatumGetPointP](../D/DatumGetPointP.md)`: Extracts point data from the entry's key
  - `gistentryinit`: Initializes a new GiST entry with the compressed data
  - `[BoxPGetDatum](../B/BoxPGetDatum.md)`: Converts box pointer to Datum for storage
  - `[GISTENTRY](../G/GISTENTRY.md)`: GiST entry structure type
  - `[BOX](../B/BOX.md)`: Bounding box structure type
  - `[Point](../P/Point.md)`: Point coordinate structure type
- Called from (representative examples):
  - Referenced by GiST access method during index operations (no direct references found in codebase)

## Notes and Other Information
- Creates degenerate bounding boxes for points where box->high equals box->low
- Only processes leaf-level entries; internal entries are passed through unchanged
- Essential for maintaining consistent bounding box representation throughout GiST point indexes
- Allocates new memory for both the box and return entry structures
- Part of the standard GiST operator class implementation for geometric point types
- The compressed format enables uniform geometric operations across different levels of the index tree