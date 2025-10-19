# LsegPGetDatum

## Location
[src/include/utils/geo_decls.h:194-197](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/geo_decls.h#L194-L197)

## Overview
LsegPGetDatum is an inline utility function that converts an LSEG (line segment) pointer to a Datum value, serving as the complementary function to DatumGetLsegP in PostgreSQL's function manager interface.

## Definition
```c
static inline Datum
LsegPGetDatum(const LSEG *X)
{
    return PointerGetDatum(X);
}
```

## Detailed Description
LsegPGetDatum is part of PostgreSQL's function manager (fmgr) interface functions for geometric data types. It provides a type-safe method to convert LSEG (line segment) pointers to Datum values, which is necessary when PostgreSQL functions need to return LSEG results through the standard Datum interface. The function acts as a wrapper around PointerGetDatum, ensuring that LSEG pointers are properly packaged as Datum values. This is the inverse operation of DatumGetLsegP and maintains the consistency of PostgreSQL's geometric data type handling.

## Parameters / Member Variables
- `X`: A const pointer to an LSEG structure to be converted to Datum

## Dependencies
- Functions called/Symbols referenced:
  - [LSEG](LSEG.md) (geometric line segment data type)
  - [PointerGetDatum](../P/PointerGetDatum.md) (implicit, through direct function call)
- Called from (representative examples):
  - PG_RETURN_LSEG_P
  - [interpt_pp](../i/interpt_pp.md) (test/regress function)

## Notes and Other Information
This function is defined as a static inline function in src/include/utils/geo_decls.h:194-197. It is primarily used through the PG_RETURN_LSEG_P macro and in some regression test functions. The function takes a const pointer, indicating that it does not modify the LSEG data. Like other geometric conversion functions, it assumes the input pointer is valid and points to a properly initialized LSEG structure. LSEG represents a line segment with two endpoints and is used in various geometric operations and spatial queries in PostgreSQL.

## Simplified Source

```c
static inline Datum
LsegPGetDatum(const LSEG *X)
{
    // Convert LSEG pointer to Datum
    return PointerGetDatum(X);
}
```