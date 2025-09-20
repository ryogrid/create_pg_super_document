# spg_bbox_quad_config

## Location
[src/backend/utils/adt/geo_spgist.c:859-875](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_spgist.c#L859-L875)

## Overview
A configuration function for SP-GiST quadtree indexes on 2D geometric types that are represented lossily by their bounding boxes.

## Definition

```c
Datum
spg_bbox_quad_config(PG_FUNCTION_ARGS)
```
## Detailed Description
This function configures SP-GiST index parameters for 2D geometric types that use bounding box representations for spatial indexing. It sets up the index to use BOX type for both prefix (internal nodes) and leaf storage, with no node labels required. The configuration is designed for lossy representation where the original geometric object is approximated by its bounding box for indexing purposes. This approach trades some precision for improved performance in spatial queries.

## Parameters / Member Variables
- Input: Standard PostgreSQL function arguments (PG_FUNCTION_ARGS)
- : spgConfigOut structure pointer containing configuration settings:
  - : Set to BOXOID for bounding box prefix storage
  - : Set to VOIDOID (no node labels needed)  
  - : Set to BOXOID for bounding box leaf storage
  - : Set to false (cannot return original data)
  - : Set to false (no support for long values)

## Dependencies
- Functions called/Symbols referenced:
  - [spgConfigOut](spgConfigOut.md) (structure type)
  - PG_RETURN_VOID (macro)
- Called from (representative examples):
  - Used as SP-GiST config function in operator class definitions

## Notes and Other Information
- Designed for lossy indexing where original geometric objects are approximated by bounding boxes
- Suitable for types like polygons, paths, and other complex 2D geometric shapes
- The lossy nature means exact object data cannot be returned from the index alone
- Part of PostgreSQL's SP-GiST framework for spatial indexing
- Typically paired with compression functions that extract bounding boxes from source objects