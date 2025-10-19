# pg_sub_s16_overflow

## Location
[src/include/common/int.h:65-82](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/int.h#L65-L82)

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
  - [int2mi](../i/int2mi.md) (16-bit integer subtraction operator function)

## Notes and Other Information
- This is a static inline function defined in src/include/common/int.h for performance
- Uses conditional compilation to prefer compiler built-ins when available
- Part of PostgreSQL's comprehensive overflow-safe arithmetic operations
- The fallback implementation uses 32-bit arithmetic to safely detect 16-bit overflow
- Returns implementation-defined result content on overflow (0x5EED) to suppress compiler warnings
- Subtraction overflow can occur when subtracting a large negative number from a positive number, or vice versa

## Simplified Source

```c
static inline bool
pg_sub_s16_overflow(int16 a, int16 b, int16 *result)
{
#if defined(HAVE__BUILTIN_OP_OVERFLOW)
    // Use compiler built-in for optimal performance
    return __builtin_sub_overflow(a, b, result);
#else
    // Manual overflow detection using 32-bit arithmetic
    int32 res = (int32) a - (int32) b;

    // Check if result exceeds 16-bit range
    if (res > PG_INT16_MAX || res < PG_INT16_MIN) {
        *result = 0x5EED;  // Dummy value to avoid warnings
        return true;  // Overflow occurred
    }

    *result = (int16) res;
    return false;  // No overflow
#endif
}
```