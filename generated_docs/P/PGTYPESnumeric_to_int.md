# PGTYPESnumeric_to_int

## Location
src/interfaces/ecpg/pgtypeslib/numeric.c: 1494 - 1517

## Overview
Converts a PostgreSQL numeric value to a C integer type, with overflow checking for platforms where long and int have different sizes.

## Definition
```c
int PGTYPESnumeric_to_int(numeric *nv, int *ip)
```

## Detailed Description
This function converts a PostgreSQL numeric value to a C int. It internally delegates to `PGTYPESnumeric_to_long` and then performs range checking to ensure the long value fits within the range of an int. On platforms where sizeof(long) > sizeof(int), it explicitly checks for overflow conditions and returns an error if the value exceeds INT_MIN or INT_MAX.

The function follows the ECPG (Embedded SQL in C) pattern of returning 0 on success and non-zero error codes on failure, with the converted value stored in the output parameter.

## Parameters / Member Variables
- `nv`: Input numeric value to convert (pointer to numeric structure)
- `ip`: Output parameter to store the converted integer value (pointer to int)

## Dependencies
- Functions called/Symbols referenced:
  - PGTYPESnumeric_to_long
  - PGTYPES_NUM_OVERFLOW (error constant)
  - numeric (type)
- Called from (representative examples):
  - dectoint (in compatlib/informix.c)
  - main (in test files for numeric operations)

## Notes and Other Information
- Returns 0 on successful conversion, non-zero on error
- On overflow, sets errno to PGTYPES_NUM_OVERFLOW and returns -1
- The overflow check is conditionally compiled based on SIZEOF_LONG vs SIZEOF_INT
- Part of the ECPG pgtypes library for embedded SQL applications
- Located in src/interfaces/ecpg/pgtypeslib/numeric.c:1494-1517