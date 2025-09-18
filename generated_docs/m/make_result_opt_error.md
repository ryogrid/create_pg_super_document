# make_result_opt_error

## Location
src/backend/utils/adt/numeric.c: 7798 - 7906

## Overview
Converts a NumericVar structure to the packed database numeric format with optional error handling for overflow conditions, supporting both normal values and special cases like NaN and Infinity.

## Definition
```c
static Numeric make_result_opt_error(const NumericVar *var, bool *have_error)
```

## Detailed Description
This function creates a packed Numeric value from a NumericVar structure, handling the conversion from the internal computational format to the storage format used in the database. The function includes comprehensive error handling for overflow conditions and supports all numeric types including special values (NaN, positive infinity, negative infinity).

The function implements several optimizations including leading and trailing zero truncation, automatic selection between short and long numeric formats based on the magnitude of weight and dscale values, and proper handling of zero values by normalizing them to positive zero with weight 0.

When overflow occurs (weight or dscale values exceed the limits of int16 fields), the function can either return NULL and set an error flag, or throw an exception depending on the have_error parameter.

## Parameters / Member Variables
- `var`: Pointer to the source NumericVar structure containing the value to be converted (const, read-only)
- `have_error`: Optional pointer to a boolean flag that will be set to true if overflow occurs (can be NULL for exception-throwing behavior)

## Dependencies
- Functions called/Symbols referenced:
  - palloc (memory allocation)
  - SET_VARSIZE (setting variable-length structure size)
  - memcpy (copying digit data)
  - dump_numeric (debugging function)
  - elog, ereport (error reporting)
  - NUMERIC_CAN_BE_SHORT (macro for format selection)
  - Various NUMERIC_* constants and macros for bit manipulation
- Called from (representative examples):
  - numeric_in
  - numeric_add_opt_error
  - numeric_sub_opt_error
  - numeric_mul_opt_error
  - numeric_div_opt_error
  - numeric_mod_opt_error
  - make_result

## Notes and Other Information
- This is a static function, only accessible within the numeric.c file
- The function automatically chooses between short and long numeric formats to optimize storage space
- Leading and trailing zeros are stripped to minimize storage requirements
- Special values (NaN, ±Infinity) are handled through a separate code path using only the header
- Zero values are normalized to positive zero with weight 0 for consistency
- The function validates special value signs to prevent corruption of reserved bits
- When have_error is NULL, overflow conditions result in exceptions; when provided, they result in graceful error handling
- The packed format uses either 16-bit or 32-bit fields depending on the chosen format (short vs long)
- Overflow detection ensures data integrity by verifying that weight and dscale values fit in their target field sizes