# ForBothCellState

## Location
src/include/nodes/pg_list.h: 86 - 92

## Overview
A state structure used by PostgreSQL's list iteration macros to maintain independent iteration positions when looping through two lists simultaneously.

## Definition
```c
typedef struct ForBothCellState
{
    const List *l1;             /* lists we're looping through */
    const List *l2;
    int         i1;             /* current element indexes */
    int         i2;
} ForBothCellState;
```

## Detailed Description
ForBothCellState provides independent iteration control for two lists, unlike ForBothState which uses a common index. This structure allows for more flexible dual-list iteration patterns where the two lists may be traversed at different rates or where different starting positions are needed. Each list maintains its own index counter, enabling asymmetric iteration patterns and more complex list processing algorithms.

## Parameters / Member Variables
- `l1`: Const pointer to the first List being iterated through
- `l2`: Const pointer to the second List being iterated through
- `i1`: Current element index for the first list
- `i2`: Current element index for the second list

## Dependencies
- Functions called/Symbols referenced:
  - [List](../L/List.md) (implicitly referenced through l1 and l2 members)
- Called from (representative examples):
  - for_both_cell
  - [for_both_cell_setup](../f/for_both_cell_setup.md)

## Notes and Other Information
This structure is more flexible than ForBothState as it allows independent progression through each list. This is useful for algorithms that need to advance through lists at different rates, start from different positions, or handle lists of different lengths in sophisticated ways. The separate index counters provide maximum flexibility for complex dual-list iteration patterns.