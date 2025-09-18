# DatumGetLsegP

## Location
[src/include/utils/geo_decls.h:189-193](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/geo_decls.h#L189-L193)

## Overview
DatumGetLsegP is an inline utility function that converts a Datum value to an LSEG (line segment) pointer, providing type-safe access to geometric line segment data in PostgreSQL's function manager interface.

## Definition
```c
static inline LSEG *
DatumGetLsegP(Datum X)
{
    return (LSEG *) DatumGetPointer(X);
}
```

## Detailed Description
DatumGetLsegP is part of PostgreSQL's function manager (fmgr) interface functions for geometric data types. It provides a convenient and type-safe method to extract LSEG (line segment) structures from Datum values. The function serves as a wrapper around DatumGetPointer, casting the result to an LSEG pointer. This function is essential for handling geometric line segment data in PostgreSQL functions that receive arguments through the Datum interface, enabling proper access to line segment coordinates and properties.

## Parameters / Member Variables
- `X`: A Datum value containing a pointer to an LSEG structure

## Dependencies
- Functions called/Symbols referenced:
  - [LSEG](../L/LSEG.md) (geometric line segment data type)
  - [DatumGetPointer](DatumGetPointer.md) (implicit, through direct pointer casting)
- Called from (representative examples):
  - PG_GETARG_LSEG_P

## Notes and Other Information
This function is defined as a static inline function in src/include/utils/geo_decls.h:189-193. Unlike some other geometric types, LSEG has relatively limited direct usage in the analyzed codebase, with its primary reference being through the PG_GETARG_LSEG_P macro. LSEG represents a line segment defined by two endpoints and is a fixed-size pass-by-reference type. The function assumes that the Datum contains a valid pointer to an LSEG structure and performs no validation. It is part of the consistent pattern of Datum conversion functions for PostgreSQL's geometric types.