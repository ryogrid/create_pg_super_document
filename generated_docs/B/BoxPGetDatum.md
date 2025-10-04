# BoxPGetDatum

## Location
[src/include/utils/geo_decls.h:239-242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/geo_decls.h#L239-L242)

## Overview
BoxPGetDatum is a static inline function that converts a BOX pointer to a PostgreSQL Datum value, enabling geometric box data to be stored and manipulated within PostgreSQL's internal data representation system.

## Definition
static inline Datum BoxPGetDatum(const BOX *X)

## Detailed Description
This function performs the reverse operation of DatumGetBoxP, converting a BOX pointer into a Datum that can be stored, passed between functions, and manipulated within PostgreSQL's type system. It uses PointerGetDatum to perform the conversion while maintaining type safety. The function is implemented as static inline for performance optimization, as it's frequently called during geometric operations, indexing, and query processing involving box data types.

## Parameters / Member Variables
- X: A const pointer to a BOX structure that needs to be converted to Datum format

## Dependencies
- Functions called/Symbols referenced:
  - [PointerGetDatum](../P/PointerGetDatum.md) (conversion utility)
  - [BOX](BOX.md) (geometric data type)
- Called from (representative examples):
  - [fallbackSplit](../f/fallbackSplit.md)
  - [gist_point_compress](../g/gist_point_compress.md)
  - [spg_kd_inner_consistent](../s/spg_kd_inner_consistent.md)
  - [spg_quad_inner_consistent](../s/spg_quad_inner_consistent.md)
  - [spg_box_quad_choose](../s/spg_box_quad_choose.md)
  - [spg_box_quad_picksplit](../s/spg_box_quad_picksplit.md)

## Notes and Other Information
This function is essential for PostgreSQL's geometric indexing infrastructure, particularly in GiST and SP-GiST implementations. It's defined in src/include/utils/geo_decls.h:239-242 and enables seamless integration between geometric data types and PostgreSQL's internal storage and retrieval mechanisms.

## Simplified Source

```c
static inline Datum
BoxPGetDatum(const BOX *X)
{
    // Convert BOX pointer to generic Datum
    // Used to package BOX data into PostgreSQL's internal format
    return PointerGetDatum(X);
}
```