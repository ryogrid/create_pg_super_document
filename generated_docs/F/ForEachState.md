# ForEachState

## Location
src/include/nodes/pg_list.h: 73 - 77

## Overview
A state structure used by PostgreSQL's list iteration macros to maintain the current position when looping through a single list.

## Definition
```c
typedef struct ForEachState
{
    const List *l;              /* list we're looping through */
    int         i;              /* current element index */
} ForEachState;
```

## Detailed Description
ForEachState is a lightweight state structure designed to support PostgreSQL's foreach iteration macros. It maintains the necessary state information for iterating through a List structure, keeping track of both the list being traversed and the current position within that list. This structure enables the implementation of safe and efficient list iteration patterns throughout the PostgreSQL codebase.

## Parameters / Member Variables
- `l`: Const pointer to the List being iterated through, ensuring the list structure itself cannot be modified during iteration
- `i`: Current element index position within the list, starting from 0 and incrementing with each iteration step

## Dependencies
- Functions called/Symbols referenced:
  - [List](../L/List.md) (implicitly referenced through the l member)
- Called from (representative examples):
  - foreach
  - for_each_from
  - for_each_cell
  - foreach_internal
  - foreach_node

## Notes and Other Information
This structure is primarily used internally by PostgreSQL's list iteration macros and is not typically manipulated directly by user code. The const qualifier on the list pointer helps prevent accidental modification of the list structure during iteration, promoting safer iteration patterns.