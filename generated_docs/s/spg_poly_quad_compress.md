# spg_poly_quad_compress

## Location
src/backend/utils/adt/geo_spgist.c: 876 - 885

## Overview
A compression function for SP-GiST quadtree indexes that extracts the bounding box from polygon geometries for spatial indexing.

## Definition


## Detailed Description
This function implements the compression step for SP-GiST quadtree indexes on polygon data types. It takes a polygon geometry as input and extracts its pre-computed bounding box (stored in the boundbox field) to create a compressed representation suitable for spatial indexing. This lossy compression allows the index to efficiently handle complex polygon shapes by representing them with simpler rectangular bounding boxes, trading some precision for improved query performance and reduced storage requirements.

## Parameters / Member Variables
- Input: Standard PostgreSQL function arguments containing:
  - : POLYGON pointer to the input polygon geometry
- Returns: BOX pointer to the extracted bounding box
- : Newly allocated BOX structure containing the polygon's bounding rectangle

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POLYGON_P (macro for extracting polygon argument)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - PG_RETURN_BOX_P (macro for returning box result)
  - [POLYGON](../P/POLYGON.md), BOX (data type structures)
- Called from (representative examples):
  - Used as SP-GiST compress function in polygon operator class definitions

## Notes and Other Information
- Performs lossy compression by reducing complex polygons to their bounding boxes
- Uses the pre-computed boundbox field from the POLYGON structure for efficiency
- Essential component of the polygon SP-GiST operator class for spatial indexing
- The compression is necessary because SP-GiST quadtree partitioning works on simpler geometric shapes
- Part of PostgreSQL's SP-GiST framework for spatial indexing
- Works in conjunction with spg_bbox_quad_config for complete polygon indexing support