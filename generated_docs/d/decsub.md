# decsub

## Location
src/interfaces/ecpg/compatlib/informix.c: 359 - 380

## Overview
Performs subtraction operation on two decimal numbers using ECPG Informix compatibility library.

## Definition
```c
int decsub(decimal *n1, decimal *n2, decimal *result)
```

## Detailed Description
The `decsub` function subtracts the second decimal number from the first (`n1` - `n2`) and stores the result in the `result` parameter. This function is part of PostgreSQL's ECPG (Embedded SQL in C) Informix compatibility library, providing compatibility with Informix database decimal arithmetic operations. The function internally uses `deccall3` helper function with `PGTYPESnumeric_sub` to perform the actual subtraction operation and handles overflow and underflow conditions that may occur during subtraction.

## Parameters / Member Variables
- `n1`: Pointer to the decimal minuend (the number from which to subtract)
- `n2`: Pointer to the decimal subtrahend (the number to subtract)
- `result`: Pointer to the decimal where the subtraction result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - deccall3
  - PGTYPESnumeric_sub
- Called from (representative examples):
  - main (in test files)
- Error constants used:
  - PGTYPES_NUM_OVERFLOW
  - ECPG_INFORMIX_NUM_OVERFLOW
  - ECPG_INFORMIX_NUM_UNDERFLOW

## Notes and Other Information
- Returns 0 on success
- Returns specific error codes for different failure conditions:
  - ECPG_INFORMIX_NUM_OVERFLOW when result overflows
  - ECPG_INFORMIX_NUM_UNDERFLOW for other numeric errors
- Sets errno internally to communicate error conditions to the caller
- Located in src/interfaces/ecpg/compatlib/informix.c:359-380
- Similar error handling pattern to multiplication, without divide-by-zero concerns