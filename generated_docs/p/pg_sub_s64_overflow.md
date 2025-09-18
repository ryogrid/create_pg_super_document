# pg_sub_s64_overflow

## Location
src/include/common/int.h: 188 - 218

## Overview
Performs subtraction of two 64-bit signed integers with overflow detection, returning true if overflow occurs and false otherwise.

## Definition
```c
static inline bool pg_sub_s64_overflow(int64 a, int64 b, int64 *result)
```

## Detailed Description
This function safely subtracts two 64-bit signed integers while detecting potential overflow conditions. Subtraction overflow is more subtle than addition overflow since subtracting a negative number is equivalent to addition. The function provides multiple implementation strategies:

1. **Built-in overflow detection**: Uses `__builtin_sub_overflow()` when available for optimal performance
2. **128-bit arithmetic**: When 128-bit integers are supported, performs subtraction in 128-bit space and checks bounds
3. **Manual overflow detection**: Uses mathematical overflow detection considering both positive and negative operand combinations

The manual implementation includes a special case comment noting that overflow can occur even when `a == 0` and `b < 0` (specifically when `b == PG_INT64_MIN`), since subtracting the most negative 64-bit value results in overflow.

## Parameters / Member Variables
- `a`: The 64-bit signed integer minuend (value to subtract from)
- `b`: The 64-bit signed integer subtrahend (value to subtract)  
- `result`: Pointer to store the subtraction result (set to 0x5EED on overflow in fallback implementations)

## Dependencies
- Constants referenced:
  - PG_INT64_MAX
  - PG_INT64_MIN
  - HAVE_INT128 (conditional compilation)
- Called from (representative examples):
  - [int8mi](../i/int8mi.md) (int8 subtraction operator)
  - [int8dec](../i/int8dec.md) (int8 decrement function)
  - [int84mi](../i/int84mi.md), int48mi, int82mi, int28mi (mixed integer type subtractions)
  - [cash_mi_cash](../c/cash_mi_cash.md) (money type subtraction)
  - [cash_in](../c/cash_in.md) (money input parsing)
  - [timestamp_mi](../t/timestamp_mi.md) (timestamp subtraction)
  - [interval_um_internal](../i/interval_um_internal.md) (interval negation)
  - [finite_interval_mi](../f/finite_interval_mi.md) (interval subtraction)
  - [TimestampDifferenceMilliseconds](../T/TimestampDifferenceMilliseconds.md) (time difference calculation)
  - [timestamp_bin](../t/timestamp_bin.md), timestamptz_bin (timestamp binning operations)
  - [numericvar_to_int64](../n/numericvar_to_int64.md) (numeric to int64 conversion)
  - [strtoint64](../s/strtoint64.md) (string to int64 parsing in pgbench)

## Notes and Other Information
- Returns `true` when overflow is detected, `false` when subtraction is safe
- On overflow, `*result` is set to 0x5EED to avoid spurious compiler warnings
- Manual overflow detection logic: `(a < 0 && b > 0 && a < PG_INT64_MIN + b) || (a >= 0 && b < 0 && a > PG_INT64_MAX + b)`
- Special attention to the edge case where subtracting PG_INT64_MIN causes overflow
- Critical for timestamp arithmetic, financial calculations, and interval operations
- The subtraction of the minimum value from zero is a classic signed integer overflow scenario
- Part of PostgreSQL's comprehensive safe arithmetic infrastructure ensuring numerical stability