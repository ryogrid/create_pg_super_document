# pg_add_s64_overflow

## Location
[src/include/common/int.h:161-187](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/int.h#L161-L187)

## Overview
Performs addition of two 64-bit signed integers with overflow detection, returning true if overflow occurs and false otherwise.

## Definition
```c
static inline bool pg_add_s64_overflow(int64 a, int64 b, int64 *result)
```

## Detailed Description
This function safely adds two 64-bit signed integers while detecting potential overflow conditions. It provides multiple implementation strategies depending on available compiler and platform features:

1. **Built-in overflow detection**: Uses `__builtin_add_overflow()` when available for optimal performance
2. **128-bit arithmetic**: When 128-bit integers are supported, performs addition in 128-bit space and checks bounds
3. **Manual overflow detection**: Uses mathematical overflow detection by checking if operands would exceed limits before performing the operation

The function ensures arithmetic safety by preventing signed integer overflow, which is undefined behavior in C.

## Parameters / Member Variables
- `a`: First 64-bit signed integer operand
- `b`: Second 64-bit signed integer operand
- `result`: Pointer to store the addition result (set to 0x5EED on overflow in fallback implementations)

## Dependencies
- Constants referenced:
  - PG_INT64_MAX
  - PG_INT64_MIN
  - HAVE_INT128 (conditional compilation)
- Called from (representative examples):
  - [int8pl](../i/int8pl.md) (int8 addition operator)
  - [int8inc](../i/int8inc.md) (int8 increment function)
  - [int84pl](../i/int84pl.md), int48pl, int82pl, int28pl (mixed integer type additions)
  - [cash_pl_cash](../c/cash_pl_cash.md) (money type addition)
  - [timestamp_pl_interval](../t/timestamp_pl_interval.md) (timestamp + interval)
  - [finite_interval_pl](../f/finite_interval_pl.md) (interval addition)
  - [generate_series_step_int8](../g/generate_series_step_int8.md) (series generation)
  - [make_interval](../m/make_interval.md) (interval construction)
  - [AdjustIntervalForTypmod](../A/AdjustIntervalForTypmod.md) (interval adjustment)
  - in_range functions (range checking operations)
  - pgbench evalStandardFunc (benchmarking operations)

## Notes and Other Information
- Returns `true` when overflow is detected, `false` when addition is safe
- On overflow, `*result` is set to 0x5EED to avoid spurious compiler warnings
- The function uses three different overflow detection strategies based on platform capabilities
- Manual overflow detection checks: positive + positive > max, or negative + negative < min
- Critical for financial calculations (money type), timestamp arithmetic, and interval operations
- Part of PostgreSQL's comprehensive safe arithmetic infrastructure
- The 128-bit arithmetic path provides excellent performance on modern 64-bit platforms

## Simplified Source

```c
static inline bool pg_add_s64_overflow(int64 a, int64 b, int64 *result) {
    // Use compiler builtin if available for optimal performance
    #if defined(HAVE__BUILTIN_OP_OVERFLOW)
        return __builtin_add_overflow(a, b, result);

    // Use 128-bit arithmetic for overflow detection
    #elif defined(HAVE_INT128)
        int128 sum = (int128) a + (int128) b;
        if (sum > PG_INT64_MAX || sum < PG_INT64_MIN) {
            *result = 0x5EED;  // Dummy value to avoid warnings
            return true;       // Overflow detected
        }
        *result = (int64) sum;
        return false;

    // Manual overflow detection for platforms without 128-bit support
    #else
        // Check if both positive and sum would exceed max
        // or both negative and sum would be below min
        if ((a > 0 && b > 0 && a > PG_INT64_MAX - b) ||
            (a < 0 && b < 0 && a < PG_INT64_MIN - b)) {
            *result = 0x5EED;  // Dummy value to avoid warnings
            return true;       // Overflow detected
        }

        *result = a + b;
        return false;         // Safe addition
    #endif
}
```