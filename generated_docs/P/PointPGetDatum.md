# PointPGetDatum

## Location
[src/include/utils/geo_decls.h:181-184](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/geo_decls.h#L181-L184)

## Overview
PointPGetDatum is an inline utility function that converts a Point pointer to a Datum value, serving as the complementary function to DatumGetPointP in PostgreSQL's function manager interface.

## Definition
```c
static inline Datum
PointPGetDatum(const Point *X)
{
    return PointerGetDatum(X);
}
```

## Detailed Description
PointPGetDatum is part of PostgreSQL's function manager (fmgr) interface functions for geometric data types. It provides a type-safe way to convert Point pointers to Datum values, which is necessary when PostgreSQL functions need to return Point results through the standard Datum interface. The function acts as a wrapper around PointerGetDatum, ensuring that Point pointers are properly packaged as Datum values. This is the inverse operation of DatumGetPointP and is essential for the proper functioning of PostgreSQL's geometric point operations.

## Parameters / Member Variables
- `X`: A const pointer to a Point structure to be converted to Datum

## Dependencies
- Functions called/Symbols referenced:
  - [Point](Point.md) (geometric point data type)
  - [PointerGetDatum](PointerGetDatum.md) (implicit, through direct function call)
- Called from (representative examples):
  - point_point_distance
  - [gist_point_consistent](../g/gist_point_consistent.md)
  - [spg_kd_choose](../s/spg_kd_choose.md)
  - [spg_kd_picksplit](../s/spg_kd_picksplit.md)
  - point_point_distance
  - SPTEST
  - [spg_quad_choose](../s/spg_quad_choose.md)
  - [spg_quad_picksplit](../s/spg_quad_picksplit.md)
  - PG_RETURN_POINT_P
  - [pt_in_widget](../p/pt_in_widget.md)

## Notes and Other Information
This function is defined as a static inline function in src/include/utils/geo_decls.h:181-184. It is widely used in spatial indexing implementations and geometric operations throughout PostgreSQL. The function takes a const pointer, indicating that it does not modify the Point data. It is commonly used in conjunction with PG_RETURN_POINT_P macro and in various geometric functions that need to return Point results. The function assumes the input pointer is valid and points to a properly initialized Point structure.