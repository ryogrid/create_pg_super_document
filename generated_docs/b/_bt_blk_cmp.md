# _bt_blk_cmp

## Location
[src/backend/access/nbtree/nbtinsert.c:3011-3017](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtinsert.c#L3011-L3017)

## Overview
A static inline comparison function used as a qsort callback for sorting arrays of BlockNumber values during B-tree simple deletion operations.

## Definition

```c
static inline int
_bt_blk_cmp(const void *arg1, const void *arg2)
```
## Detailed Description
 is a specialized comparison function designed specifically for use with qsort() to sort arrays of BlockNumber values. The function is used internally by the B-tree simple deletion mechanism, particularly in , to maintain sorted arrays of table block numbers for efficient binary search operations.

The function follows the standard qsort comparison interface, taking two void pointers that are cast to BlockNumber pointers, dereferenced, and compared using PostgreSQL's standard unsigned 32-bit integer comparison function . This ensures consistent, platform-independent comparison semantics.

The sorted block number arrays enable efficient binary search during the simple deletion pass, where the system needs to quickly determine whether a given table block is among those referenced by LP_DEAD-marked index tuples.

## Parameters / Member Variables
- : Pointer to the first BlockNumber value to compare (cast from void*)
- : Pointer to the second BlockNumber value to compare (cast from void*)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_cmp_u32](../p/pg_cmp_u32.md)
- Called from (representative examples):
  - [_bt_simpledel_pass](_bt_simpledel_pass.md) (used with bsearch for binary search operations)
  - [_bt_deadblocks](_bt_deadblocks.md) (used with qsort for sorting block arrays)

## Notes and Other Information
- This function is marked as  for performance optimization since it's called frequently during sorting operations
- Used exclusively within the B-tree access method's simple deletion mechanism
- The function maintains the qsort comparison contract: returns negative, zero, or positive values for less-than, equal-to, or greater-than relationships respectively
- Part of the optimization strategy for B-tree tuple deletion that groups operations by table block numbers to minimize I/O
- Located in src/backend/access/nbtree/nbtinsert.c:3011-3017