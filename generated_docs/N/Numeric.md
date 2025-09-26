# Numeric

## Location
[src/include/utils/numeric.h:54-60](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/numeric.h#L54-L60)

## Overview
Numeric is a typedef that represents PostgreSQL's exact numeric data type, defined as a pointer to the NumericData structure. It serves as the primary interface for PostgreSQL's arbitrary precision decimal numbers.

## Definition

```c
typedef struct NumericData *Numeric;
```
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
  - [NumericData](NumericData.md) (the underlying structure)
  - NumericChoice (union for format selection)
  - [NumericShort](NumericShort.md) (compact format)
  - [NumericLong](NumericLong.md) (extended format)
  - PG_DETOAST_DATUM (for extracting from Datum)
  - [PointerGetDatum](../P/PointerGetDatum.md) (for converting to Datum)

- Called from (representative examples):
  - [numeric_in](../n/numeric_in.md) (input function)
  - [numeric_out](../n/numeric_out.md) (output function) 
  - [numeric_add](../n/numeric_add.md), numeric_sub, numeric_mul, numeric_div (arithmetic operations)
  - [numeric_cmp](../n/numeric_cmp.md), numeric_eq, numeric_lt, etc. (comparison functions)
  - [numeric_sqrt](../n/numeric_sqrt.md), numeric_exp, numeric_ln (mathematical functions)
  - [DatumGetNumeric](../D/DatumGetNumeric.md), NumericGetDatum (conversion macros)
  - Over 200 other functions throughout the PostgreSQL codebase

## Notes and Other Information
- The Numeric type is implemented as a TOAST-able variable-length type
- Values are automatically converted between short and long formats as needed
- The implementation provides both error-throwing and non-throwing variants of arithmetic operations
- [Numeric](Numeric.md) values maintain exact precision, making them ideal for monetary calculations
- The type supports PostgreSQL's full range of SQL numeric operations and functions
- Memory management follows PostgreSQL's palloc/pfree conventions
- The format is optimized for common use cases while supporting extreme precision when needed
- Cross-platform compatibility is maintained through careful byte ordering considerations