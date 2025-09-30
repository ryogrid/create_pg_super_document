# free_var

## Location
[src/backend/utils/adt/numeric.c:6985-7000](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L6985-L7000)

## Overview
A static utility function that returns the digit buffer of a NumericVar variable to the free pool and resets the variable to an invalid state.

## Definition

```c
static void
free_var(NumericVar *var)
```
## Detailed Description
The  function is a memory management utility in PostgreSQL's numeric data type implementation. It performs cleanup operations on a NumericVar structure by freeing its allocated digit buffer and resetting the variable to a safe, invalid state. This function is essential for preventing memory leaks in numeric operations and ensuring that variables are properly cleaned up after use. The function sets the variable's sign to NUMERIC_NAN to indicate an invalid/uninitialized state, which helps catch potential use-after-free bugs.

## Parameters / Member Variables
- : Pointer to the NumericVar structure to be freed and reset

## Dependencies
- Functions called/Symbols referenced:
  - digitbuf_free
  - NUMERIC_NAN
- Called from (representative examples):
  - [numeric_in](../n/numeric_in.md)
  - [numeric_recv](../n/numeric_recv.md)
  - [numeric_round](../n/numeric_round.md)
  - [numeric_add_opt_error](../n/numeric_add_opt_error.md)
  - [numeric_sub_opt_error](../n/numeric_sub_opt_error.md)
  - [numeric_mul_opt_error](../n/numeric_mul_opt_error.md)
  - [numeric_div_opt_error](../n/numeric_div_opt_error.md)
  - [numeric_sqrt](../n/numeric_sqrt.md)
  - [numeric_power](../n/numeric_power.md)
  - [set_var_from_non_decimal_integer_str](../s/set_var_from_non_decimal_integer_str.md)

## Notes and Other Information
This function is used extensively throughout the numeric module for cleanup operations. It ensures that after freeing a variable, it cannot be accidentally used again by setting its sign to NUMERIC_NAN. The function is static and only accessible within the numeric.c file, indicating it's an internal implementation detail of the numeric type system.

## Simplified Source

```c
static void
free_var(NumericVar *var)
{
    // Free the digit buffer and reset variable to invalid state
    digitbuf_free(var->buf);
    var->buf = NULL;
    var->digits = NULL;
    var->sign = NUMERIC_NAN;  // Mark as invalid to catch use-after-free
}
```