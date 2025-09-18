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
  - palloc (for memory allocation)
  - memcpy (for copying data)
  - VARSIZE (macro for determining variable-length structure size)
  - Numeric (data type)
- Called from (representative examples):
  - numeric (conversion functions)
  - numeric_abs
  - numeric_uminus
  - numeric_uplus
  - numeric_round
  - numeric_trunc
  - numeric_ceil
  - numeric_floor
  - numeric_mod_opt_error
  - numeric_inc
  - numeric_sqrt
  - numeric_exp
  - numeric_ln
  - numeric_trim_scale

## Notes and Other Information
- This is a static function, only accessible within the numeric.c file
- The function performs a shallow memory copy but creates a deep copy since Numeric values don't contain pointers to other allocated memory
- Handles all special numeric values (NaN, Infinity) without special case handling due to the binary copy approach
- Uses PostgreSQL's variable-length structure system, where VARSIZE includes the header information
- Commonly used when numeric operations need to return a modified copy without altering the original value
- The returned Numeric value is allocated in the current memory context and will be freed when that context is destroyed