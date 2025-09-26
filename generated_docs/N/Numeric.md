# Numeric

## Location
src/include/utils/numeric.h: 54 - 60

## Overview
Numeric is a typedef that represents PostgreSQL's exact numeric data type, defined as a pointer to the NumericData structure. It serves as the primary interface for PostgreSQL's arbitrary precision decimal numbers.

## Definition


## Detailed Description
The Numeric type is PostgreSQL's implementation of arbitrary precision decimal arithmetic. It is designed to provide exact numeric calculations without the precision limitations and rounding errors inherent in floating-point representations. The type supports a wide range of precision and scale values, making it suitable for financial calculations, scientific computing, and any application requiring exact decimal arithmetic.

The actual numeric data is stored in a variable-length structure (varlena) that can use either a compact 2-byte header format (NumericShort) for commonly-encountered values or a full 4-byte header format (NumericLong) for values requiring extended precision or scale. The implementation also supports special values like NaN (Not a Number) and Infinity.

Key characteristics:
- Variable precision up to 1000 digits
- Scale ranging from -1000 to +1000  
- Supports standard arithmetic operations (+, -, *, /, %)
- Includes mathematical functions (sqrt, exp, ln, power, etc.)
- Provides comparison operations and hashing
- Optimized storage with short and long formats
- TOAST-able for large values

The numeric format uses a base-NBASE (10000) digit representation internally, with leading and trailing zeros stripped for optimal storage. The sign, scale, and weight information is encoded in the header to minimize space usage.

## Parameters / Member Variables
The Numeric typedef itself has no direct members, but it points to NumericData which contains:
- : Variable-length header (standard varlena field)
- : Union containing either NumericShort or NumericLong format data

## Dependencies
- Functions called/Symbols referenced:
  - NumericData (the underlying structure)
  - NumericChoice (union for format selection)
  - NumericShort (compact format)
  - NumericLong (extended format)
  - PG_DETOAST_DATUM (for extracting from Datum)
  - PointerGetDatum (for converting to Datum)

- Called from (representative examples):
  - numeric_in (input function)
  - numeric_out (output function) 
  - numeric_add, numeric_sub, numeric_mul, numeric_div (arithmetic operations)
  - numeric_cmp, numeric_eq, numeric_lt, etc. (comparison functions)
  - numeric_sqrt, numeric_exp, numeric_ln (mathematical functions)
  - DatumGetNumeric, NumericGetDatum (conversion macros)
  - Over 200 other functions throughout the PostgreSQL codebase

## Notes and Other Information
- The Numeric type is implemented as a TOAST-able variable-length type
- Values are automatically converted between short and long formats as needed
- The implementation provides both error-throwing and non-throwing variants of arithmetic operations
- Numeric values maintain exact precision, making them ideal for monetary calculations
- The type supports PostgreSQL's full range of SQL numeric operations and functions
- Memory management follows PostgreSQL's palloc/pfree conventions
- The format is optimized for common use cases while supporting extreme precision when needed
- Cross-platform compatibility is maintained through careful byte ordering considerations