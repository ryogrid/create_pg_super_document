# ForBothState

## Location
src/include/nodes/pg_list.h: 79 - 84

## Overview
A state structure used by PostgreSQL's list iteration macros to maintain synchronized iteration through two lists simultaneously.

## Definition
```c
typedef struct ForBothState
{
    const List *l1;             /* lists we're looping through */
    const List *l2;
    int         i;              /* common element index */
} ForBothState;
```

## Detailed Description
ForBothState enables synchronized iteration through two lists simultaneously, maintaining a common index position for both lists. This structure is essential for operations that need to process corresponding elements from two related lists in parallel, such as comparing, merging, or transforming paired data. The structure ensures that both lists are traversed at the same pace using a single index counter.

## Parameters / Member Variables
- `l1`: Const pointer to the first List being iterated through
- `l2`: Const pointer to the second List being iterated through  
- `i`: Common element index used for both lists, ensuring synchronized traversal

## Dependencies
- Functions called/Symbols referenced:
  - [List](../L/List.md) (implicitly referenced through l1 and l2 members)
- Called from (representative examples):
  - forboth

## Notes and Other Information
This structure assumes that both lists should be traversed synchronously using the same index. It's the caller's responsibility to ensure that the two lists have compatible lengths or to handle cases where they differ in size. The const qualifiers on both list pointers prevent accidental modification during iteration.