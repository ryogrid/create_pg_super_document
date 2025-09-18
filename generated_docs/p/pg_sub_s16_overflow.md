# pg_sub_s16_overflow

## Location
src/include/common/int.h: 65 - 82

## Overview
A safe integer subtraction function that performs overflow checking for 16-bit signed integers, returning true if overflow occurs and storing the result if no overflow is detected.

## Definition
```c
static inline bool pg_sub_s16_overflow(int16 a, int16 b, int16 *result)
```

## Detailed Description
This function provides safe subtraction of two 16-bit signed integers with overflow detection. Like its addition counterpart, it uses compiler built-in overflow detection when available (`__builtin_sub_overflow`) for optimal performance. When built-ins are not available, it falls back to a manual implementation that promotes the operands to 32-bit integers to detect overflow by comparing the result against the 16-bit integer range limits.

The function follows PostgreSQL's overflow checking guidelines: if overflow occurs, it returns true and the content of *result is implementation-defined (set to 0x5EED in the fallback implementation to avoid spurious compiler warnings). If no overflow occurs, it stores the correct difference in *result and returns false.

## Parameters / Member Variables
- `a`: First 16-bit signed integer operand (minuend)
- `b`: Second 16-bit signed integer operand (subtrahend)
- `result`: Pointer to store the subtraction result if no overflow occurs

## Dependencies
- Functions called/Symbols referenced:
  - PG_INT16_MAX (constant defining maximum 16-bit signed integer value)
  - PG_INT16_MIN (constant defining minimum 16-bit signed integer value)
  - `__builtin_sub_overflow` (compiler built-in, when available)
- Called from (representative examples):
  - int2mi (16-bit integer subtraction operator function)

## Notes and Other Information
- This is a static inline function defined in src/include/common/int.h for performance
- Uses conditional compilation to prefer compiler built-ins when available
- Part of PostgreSQL's comprehensive overflow-safe arithmetic operations
- The fallback implementation uses 32-bit arithmetic to safely detect 16-bit overflow
- Returns implementation-defined result content on overflow (0x5EED) to suppress compiler warnings
- Subtraction overflow can occur when subtracting a large negative number from a positive number, or vice versa