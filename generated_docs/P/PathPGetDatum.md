# PathPGetDatum

## Location
[src/include/utils/geo_decls.h:212-215](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/geo_decls.h#L212-L215)

## Overview
PathPGetDatum is an inline function that converts a PATH pointer to a PostgreSQL Datum value for returning from functions or storing in the database.

## Definition
```c
static inline Datum
PathPGetDatum(const PATH *X)
```

## Detailed Description
This function serves as a type-safe wrapper for converting PATH geometry pointers to PostgreSQL Datum values. It uses the PointerGetDatum macro to convert the PATH pointer to a Datum, which is the universal data type used by PostgreSQL's function call interface. This is the reverse operation of DatumGetPathP and is essential for returning PATH values from PostgreSQL functions.

## Parameters / Member Variables
- `X`: A const pointer to a PATH geometry object to be converted to Datum

## Dependencies
- Functions called/Symbols referenced:
  - [PointerGetDatum](PointerGetDatum.md) (macro for pointer to Datum conversion)
  - [PATH](PATH.md) (geometric data type)
- Called from (representative examples):
  - PG_RETURN_PATH_P (macro for returning PATH values from functions)

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- Part of PostgreSQL's geometric data type conversion utilities
- Takes a const pointer, indicating the PATH object is not modified during conversion
- Essential for the PostgreSQL function call interface when returning PATH values
- Complementary function to DatumGetPathP, providing bidirectional conversion