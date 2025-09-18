# pg_cmp_size

## Location
src/include/common/int.h: 507 - 512

## Overview
A fast inline comparison function for size_t values that returns a standardized comparison result (-1, 0, or 1) without using conditional branches.

## Definition
```c
static inline int
pg_cmp_size(size_t a, size_t b)
```

## Detailed Description
The `pg_cmp_size` function implements a three-way comparison for size_t values using a branchless algorithm. It returns -1 if `a < b`, 0 if `a == b`, and 1 if `a > b`. The implementation uses the expression `(a > b) - (a < b)` which leverages the fact that boolean expressions evaluate to 0 or 1 in C, creating an efficient branchless comparison that avoids conditional jumps and potential pipeline stalls.

This function is designed for comparing size_t values, which are typically used for memory sizes, array lengths, and other platform-dependent unsigned integer quantities. The size_t type varies in size depending on the platform architecture (typically 32 or 64 bits).

## Parameters / Member Variables
- `a`: First size_t value to compare
- `b`: Second size_t value to compare

## Dependencies
- Functions called/Symbols referenced:
  - (None - uses only basic arithmetic operations)
- Called from (representative examples):
  - library_name_compare (src/bin/pg_upgrade/function.c:38)

## Notes and Other Information
- The branchless implementation `(a > b) - (a < b)` is more efficient than traditional if-else comparison logic
- This function is declared as `static inline` for maximum performance in hot code paths
- Part of a family of comparison functions for different integer types, providing consistent comparison semantics
- Particularly useful for sorting operations involving memory sizes or array dimensions
- The size_t type ensures platform-appropriate sizing, making this function portable across different architectures