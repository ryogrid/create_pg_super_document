# ForThreeState

## Location
src/include/nodes/pg_list.h: 94 - 100

## Overview
A state structure used by PostgreSQL's list iteration macros to maintain synchronized iteration through three lists simultaneously using a common index.

## Definition
```c
typedef struct ForThreeState
{
    const List *l1;             /* lists we're looping through */
    const List *l2;
    const List *l3;
    int         i;              /* common element index */
} ForThreeState;
```

## Detailed Description
ForThreeState extends the concept of synchronized list iteration to three lists, maintaining a single common index for all three lists. This structure is designed for operations that need to process corresponding elements from three related lists in parallel, such as three-way comparisons, merges, or transformations involving triplets of related data. All three lists are traversed synchronously using the same index counter.

## Parameters / Member Variables
- `l1`: Const pointer to the first List being iterated through
- `l2`: Const pointer to the second List being iterated through
- `l3`: Const pointer to the third List being iterated through
- `i`: Common element index used for all three lists, ensuring synchronized traversal

## Dependencies
- Functions called/Symbols referenced:
  - [List](../L/List.md) (implicitly referenced through l1, l2, and l3 members)
- Called from (representative examples):
  - forthree

## Notes and Other Information
This structure assumes that all three lists should be traversed synchronously using the same index, similar to ForBothState but extended to three lists. It's the caller's responsibility to ensure that the three lists have compatible lengths or to handle cases where they differ in size. The const qualifiers on all list pointers prevent accidental modification during iteration. This pattern can be extended conceptually to more lists if needed, though PostgreSQL currently provides built-in support up to three lists.