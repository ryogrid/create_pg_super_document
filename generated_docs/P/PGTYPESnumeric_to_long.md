# PGTYPESnumeric_to_long

## Location
src/interfaces/ecpg/pgtypeslib/numeric.c: 1518 - 1546

## Overview
Converts a PostgreSQL numeric value to a C long integer type using string conversion and range checking.

## Definition
```c
int PGTYPESnumeric_to_long(numeric *nv, long *lp)
```

## Detailed Description
This function converts a PostgreSQL numeric value to a C long integer. It first converts the numeric to a string representation using `PGTYPESnumeric_to_asc`, then uses the standard C library function `strtol` to parse the string into a long value. The function includes comprehensive error handling for conversion failures and range overflow/underflow conditions.

When an ERANGE error occurs during strtol conversion, the function maps it to PostgreSQL-specific error codes: PGTYPES_NUM_UNDERFLOW for LONG_MIN values and PGTYPES_NUM_OVERFLOW for LONG_MAX values.

## Parameters / Member Variables
- `nv`: Input numeric value to convert (pointer to numeric structure)
- `lp`: Output parameter to store the converted long value (pointer to long)

## Dependencies
- Functions called/Symbols referenced:
  - PGTYPESnumeric_to_asc
  - PGTYPES_NUM_UNDERFLOW (error constant)
  - PGTYPES_NUM_OVERFLOW (error constant)
  - numeric (type)
  - strtol (standard C library function)
- Called from (representative examples):
  - PGTYPESnumeric_to_int
  - dectolong (in compatlib/informix.c)
  - main (in test files for numeric operations)

## Notes and Other Information
- Returns 0 on successful conversion, -1 on error
- Uses string conversion as an intermediate step rather than direct numeric manipulation
- Properly handles memory management by freeing the temporary string
- Sets errno to appropriate PGTYPES error codes on overflow/underflow
- Part of the ECPG pgtypes library for embedded SQL applications
- Located in src/interfaces/ecpg/pgtypeslib/numeric.c:1518-1546