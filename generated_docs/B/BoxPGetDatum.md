# BoxPGetDatum

## Location
src/include/utils/geo_decls.h: 239 - 242

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
  - PointerGetDatum (conversion utility)
  - BOX (geometric data type)
- Called from (representative examples):
  - fallbackSplit
  - gist_point_compress
  - spg_kd_inner_consistent
  - spg_quad_inner_consistent
  - spg_box_quad_choose
  - spg_box_quad_picksplit

## Notes and Other Information
This function is essential for PostgreSQL's geometric indexing infrastructure, particularly in GiST and SP-GiST implementations. It's defined in src/include/utils/geo_decls.h:239-242 and enables seamless integration between geometric data types and PostgreSQL's internal storage and retrieval mechanisms.