# LinePGetDatum

## Location
src/include/utils/geo_decls.h: 226 - 229

## Overview
LinePGetDatum is an inline function that converts a LINE pointer to a PostgreSQL Datum value for returning from functions or storing in the database.

## Definition
```c
static inline Datum
LinePGetDatum(const LINE *X)
```

## Detailed Description
This function serves as a type-safe wrapper for converting LINE geometry pointers to PostgreSQL Datum values. It uses the PointerGetDatum macro to convert the LINE pointer to a Datum, which is the universal data type used by PostgreSQL's function call interface. This is the reverse operation of DatumGetLineP and is essential for returning LINE values from PostgreSQL geometric functions that work with infinite lines in 2D space.

## Parameters / Member Variables
- `X`: A const pointer to a LINE geometry object to be converted to Datum

## Dependencies
- Functions called/Symbols referenced:
  - PointerGetDatum (macro for pointer to Datum conversion)
  - LINE (geometric data type for infinite lines)
- Called from (representative examples):
  - PG_RETURN_LINE_P (macro for returning LINE values from functions)

## Notes and Other Information
- This is an inline function defined in the header file for performance optimization
- Part of PostgreSQL's geometric data type conversion utilities
- Takes a const pointer, indicating the LINE object is not modified during conversion
- Essential for the PostgreSQL function call interface when returning LINE values
- Complementary function to DatumGetLineP, providing bidirectional conversion
- Used with LINE objects representing infinite lines in 2D coordinate space