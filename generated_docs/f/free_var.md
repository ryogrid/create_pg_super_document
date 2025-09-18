# free_var

## Location
src/backend/utils/adt/numeric.c: 6985 - 7000

## Overview
A static utility function that returns the digit buffer of a NumericVar variable to the free pool and resets the variable to an invalid state.

## Definition


## Detailed Description
The  function is a memory management utility in PostgreSQL's numeric data type implementation. It performs cleanup operations on a NumericVar structure by freeing its allocated digit buffer and resetting the variable to a safe, invalid state. This function is essential for preventing memory leaks in numeric operations and ensuring that variables are properly cleaned up after use. The function sets the variable's sign to NUMERIC_NAN to indicate an invalid/uninitialized state, which helps catch potential use-after-free bugs.

## Parameters / Member Variables
- : Pointer to the NumericVar structure to be freed and reset

## Dependencies
- Functions called/Symbols referenced:
  - digitbuf_free
  - NUMERIC_NAN
- Called from (representative examples):
  - numeric_in
  - numeric_recv
  - numeric_round
  - numeric_add_opt_error
  - numeric_sub_opt_error
  - numeric_mul_opt_error
  - numeric_div_opt_error
  - numeric_sqrt
  - numeric_power
  - set_var_from_non_decimal_integer_str

## Notes and Other Information
This function is used extensively throughout the numeric module for cleanup operations. It ensures that after freeing a variable, it cannot be accidentally used again by setting its sign to NUMERIC_NAN. The function is static and only accessible within the numeric.c file, indicating it's an internal implementation detail of the numeric type system.