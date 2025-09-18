# pg_sub_s32_overflow

## Location
src/include/common/int.h: 122 - 139

## Overview
A safe integer subtraction function that performs overflow checking for 32-bit signed integers, returning true if overflow occurs and storing the result if no overflow is detected.

## Definition
```c
static inline bool pg_sub_s32_overflow(int32 a, int32 b, int32 *result)
```

## Detailed Description
This function provides safe subtraction of two 32-bit signed integers with overflow detection. It follows the same pattern as the other 32-bit overflow functions, using compiler built-in overflow detection when available (`__builtin_sub_overflow`) for optimal performance. When built-ins are not available, it falls back to a manual implementation that promotes the operands to 64-bit integers to detect overflow by comparing the result against the 32-bit integer range limits.

The function follows PostgreSQL's overflow checking guidelines: if overflow occurs, it returns true and the content of *result is implementation-defined (set to 0x5EED in the fallback implementation to avoid spurious compiler warnings). If no overflow occurs, it stores the correct difference in *result and returns false.

## Parameters / Member Variables
- `a`: First 32-bit signed integer operand (minuend)
- `b`: Second 32-bit signed integer operand (subtrahend)
- `result`: Pointer to store the subtraction result if no overflow occurs

## Dependencies
- Functions called/Symbols referenced:
  - PG_INT32_MAX (constant defining maximum 32-bit signed integer value)
  - PG_INT32_MIN (constant defining minimum 32-bit signed integer value)
  - `__builtin_sub_overflow` (compiler built-in, when available)
- Called from (representative examples):
  - int4mi (32-bit integer subtraction operator function)
  - int24mi (mixed 16-bit and 32-bit integer subtraction)
  - int42mi (mixed 32-bit and 16-bit integer subtraction)
  - array_prepend (array prepend operations)
  - Various array manipulation functions (array_set_element, array_set_slice)
  - interval_um_internal (interval unary minus operations)
  - finite_interval_mi (finite interval subtraction)

## Notes and Other Information
- This is a static inline function defined in src/include/common/int.h for performance
- Uses conditional compilation to prefer compiler built-ins when available
- Part of PostgreSQL's comprehensive overflow-safe arithmetic operations
- The fallback implementation uses 64-bit arithmetic to safely detect 32-bit overflow
- Returns implementation-defined result content on overflow (0x5EED) to suppress compiler warnings
- Commonly used in array operations and date/time interval calculations
- Subtraction overflow can occur when subtracting a large negative number from a positive number (resulting in a value too large), or subtracting a large positive number from a negative number (resulting in a value too small)
- Essential for safe arithmetic in PostgreSQL's array indexing and interval arithmetic operations