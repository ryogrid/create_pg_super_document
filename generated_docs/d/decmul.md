# decmul

## Location
src/interfaces/ecpg/compatlib/informix.c: 337 - 358

## Overview
Performs multiplication operation on two decimal numbers using ECPG Informix compatibility library.

## Definition
```c
int decmul(decimal *n1, decimal *n2, decimal *result)
```

## Detailed Description
The `decmul` function multiplies two decimal numbers (`n1` * `n2`) and stores the result in the `result` parameter. This function is part of PostgreSQL's ECPG (Embedded SQL in C) Informix compatibility library, providing compatibility with Informix database decimal arithmetic operations. The function internally uses `deccall3` helper function with `PGTYPESnumeric_mul` to perform the actual multiplication operation and handles overflow and underflow conditions that may occur during multiplication.

## Parameters / Member Variables
- `n1`: Pointer to the first decimal multiplicand
- `n2`: Pointer to the second decimal multiplicand
- `result`: Pointer to the decimal where the multiplication result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - deccall3
  - PGTYPESnumeric_mul
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
- Located in src/interfaces/ecpg/compatlib/informix.c:337-358
- Unlike division, multiplication does not need to handle divide-by-zero errors