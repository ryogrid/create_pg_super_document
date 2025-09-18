# dectolong

## Location
src/interfaces/ecpg/compatlib/informix.c: 480 - 507

## Overview
Converts a decimal type to a long integer, providing Informix compatibility functionality in PostgreSQL's ECPG interface.

## Definition
```c
int dectolong(decimal *np, long *lngp)
```

## Detailed Description
The `dectolong` function is part of PostgreSQL's ECPG (Embedded SQL in C) compatibility library for Informix. It converts a decimal value to a long integer by first converting the decimal to PostgreSQL's internal numeric representation, then extracting the long integer value. Similar to `dectoint`, this function handles memory allocation, error checking, and proper cleanup of resources, but targets the larger long integer data type instead of int.

## Parameters / Member Variables
- `np`: Pointer to the input decimal value to be converted
- `lngp`: Pointer to the long integer variable where the converted result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - PGTYPESnumeric_new
  - PGTYPESnumeric_from_decimal  
  - PGTYPESnumeric_to_long
  - PGTYPESnumeric_free
  - ECPG_INFORMIX_OUT_OF_MEMORY (error constant)
  - ECPG_INFORMIX_NUM_OVERFLOW (error constant)
  - PGTYPES_NUM_OVERFLOW (error constant)
- Called from (representative examples):
  - main (in test programs)
  - ECPG_INFORMIX_EXTRA_CHARS (referenced in header)

## Notes and Other Information
- Returns 0 on success, error codes on failure
- Handles memory allocation failures gracefully
- Converts numeric overflow errors from PostgreSQL types to Informix-compatible error codes
- Part of the Informix compatibility layer in PostgreSQL ECPG
- Located in src/interfaces/ecpg/compatlib/informix.c:480-507
- Uses errno to detect overflow conditions in the underlying numeric conversion
- Companion function to `dectoint` but for long integer conversions