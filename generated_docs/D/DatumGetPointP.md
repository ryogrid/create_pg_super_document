# DatumGetPointP

## Location
src/include/utils/geo_decls.h: 176 - 180

## Overview
DatumGetPointP is an inline utility function that converts a Datum value to a Point pointer, serving as a type-safe wrapper for geometric point data extraction in PostgreSQL's function manager interface.

## Definition
```c
static inline Point *
DatumGetPointP(Datum X)
{
    return (Point *) DatumGetPointer(X);
}
```

## Detailed Description
DatumGetPointP is part of PostgreSQL's function manager (fmgr) interface functions for geometric data types. It provides a convenient and type-safe way to extract Point structures from Datum values. The function acts as a wrapper around DatumGetPointer, casting the result to a Point pointer. This function is essential for handling geometric point data in PostgreSQL functions that receive arguments through the Datum interface, which is the standard way parameters are passed in PostgreSQL's function call mechanism.

## Parameters / Member Variables
- `X`: A Datum value containing a pointer to a Point structure

## Dependencies
- Functions called/Symbols referenced:
  - [Point](../P/Point.md) (geometric point data type)
  - [DatumGetPointer](DatumGetPointer.md) (implicit, through direct pointer casting)
- Called from (representative examples):
  - [gist_point_compress](../g/gist_point_compress.md)
  - [gist_bbox_distance](../g/gist_bbox_distance.md)
  - [spg_kd_choose](../s/spg_kd_choose.md)
  - [spg_kd_picksplit](../s/spg_kd_picksplit.md)
  - [spg_kd_inner_consistent](../s/spg_kd_inner_consistent.md)
  - [spg_key_orderbys_distances](../s/spg_key_orderbys_distances.md)
  - [spg_quad_choose](../s/spg_quad_choose.md)
  - [spg_quad_picksplit](../s/spg_quad_picksplit.md)
  - [spg_quad_inner_consistent](../s/spg_quad_inner_consistent.md)
  - [spg_quad_leaf_consistent](../s/spg_quad_leaf_consistent.md)
  - [spg_box_quad_inner_consistent](../s/spg_box_quad_inner_consistent.md)
  - PG_GETARG_POINT_P

## Notes and Other Information
This function is defined as a static inline function in src/include/utils/geo_decls.h:176-180. It is extensively used in spatial indexing implementations (GiST and SP-GiST) and throughout the geometric operations codebase. The function assumes that the Datum contains a valid pointer to a Point structure - no validation is performed. Point is a fixed-size pass-by-reference type, unlike Path and Polygon which are toastable varlena types.