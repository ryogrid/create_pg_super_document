# decadd

## Location
src/interfaces/ecpg/compatlib/informix.c: 151 - 166

## Overview
Performs addition of two decimal numbers, providing Informix-compatible decimal arithmetic with proper overflow/underflow handling and error reporting.

## Definition
```c
int decadd(decimal *arg1, decimal *arg2, decimal *sum)
```

## Detailed Description
The `decadd` function implements decimal addition for Informix compatibility in ECPG. It uses the internal `deccall3` helper function to perform the actual addition via `PGTYPESnumeric_add`, converting between decimal and numeric types as needed. The function provides comprehensive error handling for numeric overflow and underflow conditions, returning appropriate Informix-compatible error codes.

## Parameters / Member Variables
- `arg1`: Pointer to the first decimal operand
- `arg2`: Pointer to the second decimal operand
- `sum`: Pointer to the decimal variable that will store the addition result

## Dependencies
- Functions called/Symbols referenced:
  - deccall3
  - PGTYPESnumeric_add
  - PGTYPES_NUM_OVERFLOW
  - PGTYPES_NUM_UNDERFLOW
  - ECPG_INFORMIX_NUM_OVERFLOW
  - ECPG_INFORMIX_NUM_UNDERFLOW
- Called from (representative examples):
  - main (in test programs)
  - ECPG applications using Informix decimal compatibility

## Notes and Other Information
- Part of the public Informix decimal compatibility API
- Returns 0 on success, positive error codes for overflow/underflow, -1 for other errors
- Automatically handles null input checking through the `deccall3` wrapper
- Uses errno to detect specific numeric operation errors
- Essential function for applications porting from Informix to PostgreSQL