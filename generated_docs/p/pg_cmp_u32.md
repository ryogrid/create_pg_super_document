# pg_cmp_u32

## Location
[src/include/common/int.h:489-494](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/int.h#L489-L494)

## Overview
A fast inline comparison function for 32-bit unsigned integers that returns a standardized comparison result (-1, 0, or 1) without using conditional branches.

## Definition
```c
static inline int
pg_cmp_u32(uint32 a, uint32 b)
```

## Detailed Description
The `pg_cmp_u32` function implements a three-way comparison for 32-bit unsigned integers using a branchless algorithm. It returns -1 if `a < b`, 0 if `a == b`, and 1 if `a > b`. The implementation uses the expression `(a > b) - (a < b)` which leverages the fact that boolean expressions evaluate to 0 or 1 in C, creating an efficient branchless comparison that avoids conditional jumps and potential pipeline stalls.

This function is particularly important for comparing unsigned values like OIDs (Object IDentifiers), block numbers, and XIDs (Transaction IDentifiers) throughout PostgreSQL's codebase.

## Parameters / Member Variables
- `a`: First 32-bit unsigned integer to compare
- `b`: Second 32-bit unsigned integer to compare

## Dependencies
- Functions called/Symbols referenced:
  - (None - uses only basic arithmetic operations)
- Called from (representative examples):
  - [list_oid_cmp](../l/list_oid_cmp.md) (src/backend/nodes/list.c:1708)
  - [oid_cmp](../o/oid_cmp.md) (src/backend/utils/adt/oid.c:263)
  - [xidComparator](../x/xidComparator.md) (src/backend/utils/adt/xid.c:144)
  - [_bt_blk_cmp](../b/_bt_blk_cmp.md) (src/backend/access/nbtree/nbtinsert.c:3016)

## Notes and Other Information
- The branchless implementation `(a > b) - (a < b)` is more efficient than traditional if-else comparison logic
- This function is declared as `static inline` for maximum performance in hot code paths
- Extensively used for comparing PostgreSQL's internal unsigned identifiers like OIDs and XIDs
- Part of a family of comparison functions for different integer types, providing consistent comparison semantics

## Simplified Source

```c
// Simplified version of pg_cmp_u32
static inline int
pg_cmp_u32(uint32 a, uint32 b)
{
    // Branchless three-way comparison: returns -1, 0, or 1
    // Uses arithmetic properties: (a > b) evaluates to 0 or 1, same for (a < b)
    return (a > b) - (a < b);
}
```

Key simplifications made:
- Function is already extremely simple and efficient
- Added explanatory comments for the branchless algorithm
- No complex logic to simplify - this is a minimal, optimized implementation