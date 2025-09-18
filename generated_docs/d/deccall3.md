# deccall3

## Location
src/interfaces/ecpg/compatlib/informix.c: 86 - 150

## Overview  
A static utility function that wraps numeric functions taking two input parameters and producing one output result, with comprehensive null handling and memory management for decimal-to-numeric conversions.

## Definition
```c
static int deccall3(decimal *arg1, decimal *arg2, decimal *result, int (*ptr)(numeric *, numeric *, numeric *))
```

## Detailed Description
The `deccall3` function is an internal helper in the ECPG Informix compatibility library that standardizes calls to numeric functions requiring two inputs and one output. It performs null checking on input arguments, converts Informix decimal types to PostgreSQL numeric types, executes the provided function, and converts the result back to decimal format. The function implements careful memory management and handles the case where the result variable might be the same as one of the input arguments.

## Parameters / Member Variables
- `arg1`: Pointer to the first decimal input argument
- `arg2`: Pointer to the second decimal input argument  
- `result`: Pointer to the decimal variable that will receive the result
- `ptr`: Function pointer to the numeric function that performs the actual operation

## Dependencies
- Functions called/Symbols referenced:
  - risnull
  - PGTYPESnumeric_new
  - PGTYPESnumeric_free
  - PGTYPESnumeric_from_decimal
  - PGTYPESnumeric_to_decimal
  - rsetnull
  - CDECIMALTYPE
  - ECPG_INFORMIX_OUT_OF_MEMORY
- Called from (representative examples):
  - decadd
  - decdiv
  - decmul
  - decsub

## Notes and Other Information
- Static function internal to the Informix compatibility library
- Returns 0 immediately if either input argument is null
- Sets result to null before attempting conversion to handle potential errors
- Properly handles the case where result variable is aliased with input arguments
- Comprehensive memory cleanup on all error paths
- Used by arithmetic operations like addition, subtraction, multiplication, and division