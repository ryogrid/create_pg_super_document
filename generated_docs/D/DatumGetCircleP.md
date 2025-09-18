# DatumGetCircleP

## Location
[src/include/utils/geo_decls.h:266-270](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/geo_decls.h#L266-L270)

## Overview
Converts a Datum value to a CIRCLE pointer, providing type-safe access to geometric circle data stored in PostgreSQLs internal format.

## Definition
```c
static inline CIRCLE *
DatumGetCircleP(Datum X)
```

## Detailed Description
DatumGetCircleP is an inline utility function that extracts a CIRCLE pointer from a Datum value. It serves as a type-safe wrapper around DatumGetPointer, specifically designed for handling geometric circle data in PostgreSQL. This function is part of the geometric data type conversion utilities that facilitate interaction between PostgreSQLs internal Datum representation and the structured CIRCLE type.

The function performs a simple cast operation, converting the generic pointer returned by DatumGetPointer into a CIRCLE-specific pointer. This conversion is safe because the caller is expected to know that the Datum contains circle data.

## Parameters / Member Variables
- `X`: The input Datum value that contains a pointer to CIRCLE data

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetPointer](DatumGetPointer.md)
  - CIRCLE (struct type)
- Called from (representative examples):
  - [gist_circle_compress](../g/gist_circle_compress.md)
  - PG_GETARG_CIRCLE_P (macro)

## Notes and Other Information
- This is a static inline function defined in src/include/utils/geo_decls.h:266-270
- Part of PostgreSQLs geometric data type conversion infrastructure
- The CIRCLE struct contains a center point (Point) and radius (float8)
- Used primarily in GiST indexing operations and function parameter extraction
- The function assumes the Datum actually contains valid CIRCLE data - no validation is performed