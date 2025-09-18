# pg_cmp_s64

## Location
src/include/common/int.h: 495 - 500

## Overview
A fast inline comparison function for 64-bit signed integers that returns a standardized comparison result (-1, 0, or 1) without using conditional branches.

## Definition
```c
static inline int
pg_cmp_s64(int64 a, int64 b)
```

## Detailed Description
The `pg_cmp_s64` function implements a three-way comparison for 64-bit signed integers using a branchless algorithm. It returns -1 if `a < b`, 0 if `a == b`, and 1 if `a > b`. The implementation uses the expression `(a > b) - (a < b)` which leverages the fact that boolean expressions evaluate to 0 or 1 in C, creating an efficient branchless comparison that avoids conditional jumps and potential pipeline stalls.

This function provides the 64-bit signed integer variant of PostgreSQL's standardized comparison utilities, designed for use with larger numeric values that require 64-bit precision.

## Parameters / Member Variables
- `a`: First 64-bit signed integer to compare
- `b`: Second 64-bit signed integer to compare

## Dependencies
- Functions called/Symbols referenced:
  - (None - uses only basic arithmetic operations)
- Called from (representative examples):
  - (Currently no direct callers found in the codebase)

## Notes and Other Information
- The branchless implementation `(a > b) - (a < b)` is more efficient than traditional if-else comparison logic
- This function is declared as `static inline` for maximum performance in hot code paths
- Part of a family of comparison functions for different integer types (pg_cmp_s32, pg_cmp_u32, pg_cmp_u64, pg_cmp_size)
- Provides consistent three-way comparison semantics for 64-bit signed integers
- Currently appears to be unused in the analyzed codebase but available for future use or external extensions