# duplicate_numeric

## Location
src/backend/utils/adt/numeric.c: 7779 - 7797

## Overview
Creates a complete copy of a packed-format Numeric value, handling all numeric types including NaN and Infinity cases.

## Definition
```c
static Numeric duplicate_numeric(Numeric num)
```

## Detailed Description
This function creates a deep copy of a Numeric value in its packed format. It allocates new memory with the exact size of the source numeric value and performs a byte-for-byte copy. The function is designed to handle all types of numeric values, including special cases like NaN (Not a Number) and Infinity values, making it a reliable utility for creating independent copies of numeric data.

The function uses PostgreSQL's memory management system (palloc) and relies on the VARSIZE macro to determine the exact size of the variable-length numeric structure, ensuring that the entire value including all digits is copied.

## Parameters / Member Variables
- `num`: The source Numeric value to be duplicated (passed by value as a pointer)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (for memory allocation)
  - memcpy (for copying data)
  - VARSIZE (macro for determining variable-length structure size)
  - Numeric (data type)
- Called from (representative examples):
  - [numeric](../n/numeric.md) (conversion functions)
  - [numeric_abs](../n/numeric_abs.md)
  - [numeric_uminus](../n/numeric_uminus.md)
  - [numeric_uplus](../n/numeric_uplus.md)
  - [numeric_round](../n/numeric_round.md)
  - [numeric_trunc](../n/numeric_trunc.md)
  - numeric_ceil
  - numeric_floor
  - [numeric_mod_opt_error](../n/numeric_mod_opt_error.md)
  - [numeric_inc](../n/numeric_inc.md)
  - [numeric_sqrt](../n/numeric_sqrt.md)
  - [numeric_exp](../n/numeric_exp.md)
  - [numeric_ln](../n/numeric_ln.md)
  - [numeric_trim_scale](../n/numeric_trim_scale.md)

## Notes and Other Information
- This is a static function, only accessible within the numeric.c file
- The function performs a shallow memory copy but creates a deep copy since Numeric values don't contain pointers to other allocated memory
- Handles all special numeric values (NaN, Infinity) without special case handling due to the binary copy approach
- Uses PostgreSQL's variable-length structure system, where VARSIZE includes the header information
- Commonly used when numeric operations need to return a modified copy without altering the original value
- The returned Numeric value is allocated in the current memory context and will be freed when that context is destroyed