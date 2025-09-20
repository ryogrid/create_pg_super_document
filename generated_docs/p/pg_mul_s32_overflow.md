# pg_mul_s32_overflow

## Location
[src/include/common/int.h:140-160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/int.h#L140-L160)

## Overview
Performs multiplication of two 32-bit signed integers with overflow detection, returning true if overflow occurs and false otherwise.

## Definition

```c
static inline bool
pg_mul_s32_overflow(int32 a, int32 b, int32 *result)
```
## Detailed Description
This function safely multiplies two 32-bit signed integers while detecting potential overflow conditions. It provides a portable overflow checking mechanism that works across different compiler environments. When available, it utilizes compiler built-in overflow detection functions for optimal performance. Otherwise, it implements overflow detection by performing the multiplication using 64-bit arithmetic and checking if the result exceeds the valid 32-bit signed integer range.

The function has two implementation paths:
1. **Built-in overflow detection**: Uses  when available (GCC/Clang)
2. **Manual overflow detection**: Performs multiplication in 64-bit space and range-checks the result

## Parameters / Member Variables
- : First 32-bit signed integer operand
- : Second 32-bit signed integer operand  
- : Pointer to store the multiplication result (set to 0x5EED on overflow in fallback implementation)

## Dependencies
- Constants referenced:
  - PG_INT32_MAX
  - PG_INT32_MIN
- Called from (representative examples):
  - [int4mul](../i/int4mul.md) (integer multiplication operator)
  - [int24mul](../i/int24mul.md) (int2 * int4 multiplication)
  - [int42mul](../i/int42mul.md) (int4 * int2 multiplication)
  - AdjustDays (datetime adjustment)
  - AdjustYears (datetime adjustment)
  - [make_interval](../m/make_interval.md) (interval creation)
  - [text_substring](../t/text_substring.md) (string substring operation)
  - [lpad](../l/lpad.md), rpad (string padding functions)
  - [repeat](../r/repeat.md) (string repetition)
  - [translate](../t/translate.md) (string character translation)

## Notes and Other Information
- Returns  when overflow is detected,  when multiplication is safe
- On overflow in the fallback implementation,  is set to 0x5EED to avoid spurious compiler warnings
- The function is declared as  for optimal performance in arithmetic-intensive operations
- This is part of PostgreSQL's safe arithmetic library that prevents integer overflow vulnerabilities
- The built-in overflow detection path provides better performance when supported by the compiler