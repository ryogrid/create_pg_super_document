# pg_cmp_u64

## Location
[src/include/common/int.h:501-506](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/int.h#L501-L506)

## Overview
A fast inline comparison function for 64-bit unsigned integers that returns a standardized comparison result (-1, 0, or 1) without using conditional branches.

## Definition
```c
static inline int
pg_cmp_u64(uint64 a, uint64 b)
```

## Detailed Description
The `pg_cmp_u64` function implements a three-way comparison for 64-bit unsigned integers using a branchless algorithm. It returns -1 if `a < b`, 0 if `a == b`, and 1 if `a > b`. The implementation uses the expression `(a > b) - (a < b)` which leverages the fact that boolean expressions evaluate to 0 or 1 in C, creating an efficient branchless comparison that avoids conditional jumps and potential pipeline stalls.

This function is particularly useful for comparing large unsigned values like LSNs (Log Sequence Numbers), timestamps, and other 64-bit identifiers used throughout PostgreSQL's replication and storage systems.

## Parameters / Member Variables
- `a`: First 64-bit unsigned integer to compare
- `b`: Second 64-bit unsigned integer to compare

## Dependencies
- Functions called/Symbols referenced:
  - (None - uses only basic arithmetic operations)
- Called from (representative examples):
  - [ListComparatorForWalSummaryFiles](../L/ListComparatorForWalSummaryFiles.md) (src/backend/backup/walsummary.c:352)
  - [file_sort_by_lsn](../f/file_sort_by_lsn.md) (src/backend/replication/logical/reorderbuffer.c:5318)
  - [cmp_lsn](../c/cmp_lsn.md) (src/backend/replication/syncrep.c:743)
  - [ginCompareItemPointers](../g/ginCompareItemPointers.md) (src/include/access/gin_private.h:493)

## Notes and Other Information
- The branchless implementation `(a > b) - (a < b)` is more efficient than traditional if-else comparison logic
- This function is declared as `static inline` for maximum performance in hot code paths
- Commonly used for comparing LSNs in replication and WAL processing where performance is critical
- Part of a family of comparison functions for different integer types, providing consistent comparison semantics
- Essential for sorting operations involving large unsigned identifiers in PostgreSQL's internal data structures