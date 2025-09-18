# dectodbl

## Location
src/interfaces/ecpg/compatlib/informix.c: 432 - 452

## Overview
Converts a decimal number to a double-precision floating-point value using ECPG Informix compatibility library.

## Definition
```c
int dectodbl(decimal *np, double *dblp)
```

## Detailed Description
The `dectodbl` function converts a decimal number to a double-precision floating-point value. This function is part of PostgreSQL's ECPG (Embedded SQL in C) Informix compatibility library, providing compatibility with Informix database decimal-to-double conversion operations. The function uses the PostgreSQL numeric type system internally for the conversion process, ensuring proper handling of precision and range limitations when converting from the potentially higher-precision decimal type to the IEEE 754 double format.

## Parameters / Member Variables
- `np`: Pointer to the decimal number to be converted
- `dblp`: Pointer to the double variable where the conversion result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - PGTYPESnumeric_new
  - PGTYPESnumeric_from_decimal
  - PGTYPESnumeric_to_double
  - PGTYPESnumeric_free
- Called from (representative examples):
  - main (in test files)
- Error constants used:
  - ECPG_INFORMIX_OUT_OF_MEMORY

## Notes and Other Information
- Returns 0 on successful conversion
- Returns ECPG_INFORMIX_OUT_OF_MEMORY when memory allocation fails during intermediate numeric operations
- Returns the error code from PGTYPESnumeric_to_double for conversion-specific errors
- Performs proper memory cleanup of intermediate numeric values
- Located in src/interfaces/ecpg/compatlib/informix.c:432-452
- May lose precision when converting from high-precision decimal to double format
- Handles the conversion through PostgreSQL's numeric type as an intermediate step