# PGTYPESnumeric_from_int

## Location
src/interfaces/ecpg/pgtypeslib/numeric.c: 1309 - 1317

## Overview
Converts a signed integer value to PostgreSQL's numeric data type in the ECPG pgtypes library.

## Definition
```c
int PGTYPESnumeric_from_int(signed int int_val, numeric *var)
```

## Detailed Description
This function serves as a simple wrapper that converts a signed integer to a numeric variable by first promoting the integer to a signed long int and then delegating the actual conversion to PGTYPESnumeric_from_long. This approach provides a consistent interface for integer-to-numeric conversion while reusing the more comprehensive long integer conversion logic. The implicit conversion to long int ensures compatibility with the underlying conversion implementation.

## Parameters / Member Variables
- `int_val`: The signed integer value to convert to numeric format
- `var`: Pointer to the numeric variable to store the converted result

## Dependencies
- Functions called/Symbols referenced:
  - [PGTYPESnumeric_from_long](PGTYPESnumeric_from_long.md) (performs the actual conversion after type promotion)
  - [numeric](../n/numeric.md) (numeric data type)
- Called from (representative examples):
  - [deccvint](../d/deccvint.md) (in ECPG Informix compatibility layer)
  - [main](../m/main.md) (in various pgtypes test programs)

## Notes and Other Information
- Returns 0 on success, -1 on failure (typically memory allocation errors)
- Provides a simple interface for converting common integer types to numeric
- Part of a family of conversion functions (from_int, from_long, from_double, etc.)
- Essential for applications that need to work with both integer and high-precision numeric data
- Commonly used in financial applications where integer values need exact decimal representation
- The conversion preserves the exact integer value without precision loss