# pg_add_s32_overflow

## Location
[src/include/common/int.h:104-121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/int.h#L104-L121)

## Overview
A safe integer addition function that performs overflow checking for 32-bit signed integers, returning true if overflow occurs and storing the result if no overflow is detected.

## Definition
```c
static inline bool pg_add_s32_overflow(int32 a, int32 b, int32 *result)
```

## Detailed Description
This function provides safe addition of two 32-bit signed integers with overflow detection. It follows the same pattern as the 16-bit overflow functions but operates on 32-bit integers. It uses compiler built-in overflow detection when available (`__builtin_add_overflow`) for optimal performance. When built-ins are not available, it falls back to a manual implementation that promotes the operands to 64-bit integers to detect overflow by comparing against the 32-bit integer range limits.

The function follows PostgreSQL's overflow checking guidelines: if overflow occurs, it returns true and the content of *result is implementation-defined (set to 0x5EED in the fallback implementation to avoid spurious compiler warnings). If no overflow occurs, it stores the correct sum in *result and returns false.

## Parameters / Member Variables
- `a`: First 32-bit signed integer operand
- `b`: Second 32-bit signed integer operand
- `result`: Pointer to store the addition result if no overflow occurs

## Dependencies
- Functions called/Symbols referenced:
  - PG_INT32_MAX (constant defining maximum 32-bit signed integer value)
  - PG_INT32_MIN (constant defining minimum 32-bit signed integer value)
  - `__builtin_add_overflow` (compiler built-in, when available)
- Called from (representative examples):
  - [int4pl](../i/int4pl.md) (32-bit integer addition operator function)
  - [int4inc](../i/int4inc.md) (32-bit integer increment function)
  - [detoast_attr_slice](../d/detoast_attr_slice.md) (TOAST attribute slicing)
  - [array_append](../a/array_append.md) (array append operations)
  - Various array manipulation functions
  - Date/time calculation functions
  - String manipulation functions (lpad, rpad, repeat, translate)

## Notes and Other Information
- This is a static inline function defined in src/include/common/int.h for performance
- Uses conditional compilation to prefer compiler built-ins when available
- Part of PostgreSQL's comprehensive overflow-safe arithmetic operations
- The fallback implementation uses 64-bit arithmetic to safely detect 32-bit overflow
- Returns implementation-defined result content on overflow (0x5EED) to suppress compiler warnings
- Widely used throughout PostgreSQL for safe integer arithmetic in array operations, date/time calculations, and string processing
- More commonly used than the 16-bit version due to 32-bit integers being the standard integer type in many contexts