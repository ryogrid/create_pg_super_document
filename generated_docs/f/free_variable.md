# free_variable

## Location
src/interfaces/ecpg/ecpglib/execute.c: 83 - 95

## Overview
A static utility function that deallocates a linked list of variable structures used in ECPG statement execution.

## Definition
```c
static void free_variable(struct variable *var)
```

## Detailed Description
The `free_variable` function iterates through a linked list of `variable` structures and deallocates each node using ECPG's memory management system. It safely traverses the entire list by storing the next pointer before freeing each node, ensuring that the linked list is completely cleaned up without memory leaks.

## Parameters / Member Variables
- `var`: Pointer to the first variable structure in a linked list to be freed

## Dependencies
- Functions called/Symbols referenced:
  - ecpg_free
- Called from (representative examples):
  - [free_statement](free_statement.md) (multiple locations)

## Notes and Other Information
- Uses safe linked list traversal pattern to avoid accessing freed memory
- Handles NULL input gracefully (the while loop simply won't execute)
- Part of ECPG's memory management system for cleaning up statement resources
- Works with the variable structure that represents parameters and host variables in prepared statements