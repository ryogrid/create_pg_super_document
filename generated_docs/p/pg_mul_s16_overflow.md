# pg_mul_s16_overflow

## Location
[src/include/common/int.h:83-103](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/int.h#L83-L103)

## Overview
A safe integer multiplication function that performs overflow checking for 16-bit signed integers, returning true if overflow occurs and storing the result if no overflow is detected.

## Definition
```c
static inline bool pg_mul_s16_overflow(int16 a, int16 b, int16 *result)
```

## Detailed Description
This function provides safe multiplication of two 16-bit signed integers with overflow detection. It follows the same pattern as the addition and subtraction overflow functions, using compiler built-in overflow detection when available (`__builtin_mul_overflow`) for optimal performance. When built-ins are not available, it falls back to a manual implementation that promotes the operands to 32-bit integers to perform the multiplication and then checks if the result exceeds the 16-bit integer range.

The function follows PostgreSQL's overflow checking guidelines: if overflow occurs, it returns true and the content of *result is implementation-defined (set to 0x5EED in the fallback implementation to avoid spurious compiler warnings). If no overflow occurs, it stores the correct product in *result and returns false.

## Parameters / Member Variables
- `a`: First 16-bit signed integer operand (multiplicand)
- `b`: Second 16-bit signed integer operand (multiplier)
- `result`: Pointer to store the multiplication result if no overflow occurs

## Dependencies
- Functions called/Symbols referenced:
  - PG_INT16_MAX (constant defining maximum 16-bit signed integer value)
  - PG_INT16_MIN (constant defining minimum 16-bit signed integer value)
  - `__builtin_mul_overflow` (compiler built-in, when available)
- Called from (representative examples):
  - [int2mul](../i/int2mul.md) (16-bit integer multiplication operator function)

## Notes and Other Information
- This is a static inline function defined in src/include/common/int.h for performance
- Uses conditional compilation to prefer compiler built-ins when available
- Part of PostgreSQL's comprehensive overflow-safe arithmetic operations
- The fallback implementation uses 32-bit arithmetic to safely detect 16-bit overflow
- Returns implementation-defined result content on overflow (0x5EED) to suppress compiler warnings
- Multiplication overflow is more likely to occur than addition/subtraction overflow due to the nature of multiplication expanding the result range

## Simplified Source

```c
static inline bool
pg_mul_s16_overflow(int16 a, int16 b, int16 *result)
{
#if defined(HAVE__BUILTIN_OP_OVERFLOW)
    // Use compiler built-in for optimal performance
    return __builtin_mul_overflow(a, b, result);
#else
    // Manual overflow detection using 32-bit arithmetic
    int32 res = (int32) a * (int32) b;

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