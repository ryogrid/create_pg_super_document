# pg_add_s64_overflow

## Location
src/include/common/int.h: 161 - 187

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
  - int8pl (int8 addition operator)
  - int8inc (int8 increment function)
  - int84pl, int48pl, int82pl, int28pl (mixed integer type additions)
  - cash_pl_cash (money type addition)
  - timestamp_pl_interval (timestamp + interval)
  - finite_interval_pl (interval addition)
  - generate_series_step_int8 (series generation)
  - make_interval (interval construction)
  - AdjustIntervalForTypmod (interval adjustment)
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