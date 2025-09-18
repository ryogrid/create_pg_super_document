# decdiv

## Location
src/interfaces/ecpg/compatlib/informix.c: 312 - 336

## Overview
Performs division operation on two decimal numbers using ECPG Informix compatibility library.

## Definition
```c
int decdiv(decimal *n1, decimal *n2, decimal *result)
```

## Detailed Description
The `decdiv` function divides two decimal numbers (`n1` / `n2`) and stores the result in the `result` parameter. This function is part of PostgreSQL's ECPG (Embedded SQL in C) Informix compatibility library, providing compatibility with Informix database decimal arithmetic operations. The function internally uses `deccall3` helper function with `PGTYPESnumeric_div` to perform the actual division operation and handles various error conditions that may occur during division.

## Parameters / Member Variables
- `n1`: Pointer to the decimal dividend (the number to be divided)
- `n2`: Pointer to the decimal divisor (the number to divide by)  
- `result`: Pointer to the decimal where the division result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [deccall3](deccall3.md)
  - [PGTYPESnumeric_div](../P/PGTYPESnumeric_div.md)
- Called from (representative examples):
  - [main](../m/main.md) (in test files)
- Error constants used:
  - PGTYPES_NUM_DIVIDE_ZERO
  - ECPG_INFORMIX_DIVIDE_ZERO
  - PGTYPES_NUM_OVERFLOW
  - ECPG_INFORMIX_NUM_OVERFLOW
  - ECPG_INFORMIX_NUM_UNDERFLOW

## Notes and Other Information
- Returns 0 on success
- Returns specific error codes for different failure conditions:
  - ECPG_INFORMIX_DIVIDE_ZERO when dividing by zero
  - ECPG_INFORMIX_NUM_OVERFLOW when result overflows
  - ECPG_INFORMIX_NUM_UNDERFLOW for other numeric errors
- Sets errno internally to communicate error conditions to the caller
- Located in src/interfaces/ecpg/compatlib/informix.c:312-336