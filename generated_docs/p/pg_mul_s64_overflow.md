# pg_mul_s64_overflow

## Location
[src/include/common/int.h:219-269](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/int.h#L219-L269)

## Overview
Performs multiplication of two 64-bit signed integers with overflow detection, returning true if overflow occurs and false otherwise.

## Definition
```c
static inline bool pg_mul_s64_overflow(int64 a, int64 b, int64 *result)
```

## Detailed Description
This function safely multiplies two 64-bit signed integers while detecting potential overflow conditions. It uses the most sophisticated overflow detection logic among the integer arithmetic functions, with three implementation strategies:

1. **Built-in overflow detection**: Uses `__builtin_mul_overflow()` when available for optimal performance
2. **128-bit arithmetic**: When 128-bit integers are supported, performs multiplication in 128-bit space and checks bounds  
3. **Manual overflow detection**: Uses an optimized algorithm that first checks if values are within the 32-bit range (sqrt approximation), then performs division-based overflow checking only for potentially problematic values

The manual implementation is particularly sophisticated, using the mathematical property that overflow can only occur if at least one operand is outside the range sqrt(min)..sqrt(max). It avoids expensive division operations by first checking if both operands fit in 32-bit range, and includes special handling for multiplication by 0, 1, and the problematic INT_MIN/-1 case.

## Parameters / Member Variables
- `a`: First 64-bit signed integer operand
- `b`: Second 64-bit signed integer operand
- `result`: Pointer to store the multiplication result (set to 0x5EED on overflow in fallback implementation)

## Dependencies
- Constants referenced:
  - PG_INT64_MAX, PG_INT64_MIN
  - PG_INT32_MAX, PG_INT32_MIN  
  - HAVE_INT128 (conditional compilation)
- Called from (representative examples):
  - [int8mul](../i/int8mul.md) (int8 multiplication operator)
  - [int84mul](../i/int84mul.md), int48mul, int82mul, int28mul (mixed integer type multiplications)
  - [int8lcm](../i/int8lcm.md) (int8 least common multiple)
  - [cash_mul_int64](../c/cash_mul_int64.md) (money multiplication)  
  - [cash_in](../c/cash_in.md) (money input parsing)
  - int64_multiply_add (datetime calculations)
  - [DecodeInterval](../D/DecodeInterval.md) (interval parsing)
  - [timestamp_bin](../t/timestamp_bin.md), timestamptz_bin (timestamp binning)
  - [int64_div_fast_to_numeric](../i/int64_div_fast_to_numeric.md) (numeric conversion)
  - [numericvar_to_int64](../n/numericvar_to_int64.md) (numeric to int64 conversion)
  - [interval_part_common](../i/interval_part_common.md) (interval extraction)
  - [strtoint64](../s/strtoint64.md) (string to int64 parsing in pgbench)

## Notes and Other Information
- Returns `true` when overflow is detected, `false` when multiplication is safe
- On overflow, `*result` is set to 0x5EED to avoid spurious compiler warnings
- Most complex manual overflow detection: first checks 32-bit bounds as optimization
- Avoids expensive division unless operands are outside 32-bit range
- Special cases: multiplication by 0 or 1 cannot overflow
- Careful handling of INT_MIN/-1 division by ensuring division by positive values
- Manual algorithm checks all four sign combinations: (+,+), (+,-), (-,+), (-,-)
- Critical for financial calculations, timestamp arithmetic, and high-precision numeric operations
- The sqrt-based optimization significantly improves performance for common cases where operands fit in 32-bit range