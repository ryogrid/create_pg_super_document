# QTNFree

## Location
[src/backend/utils/adt/tsquery_util.c:64-96](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_util.c#L64-L96)

## Overview
Recursively frees a QTNode tree structure and its associated memory, handling both the tree nodes and their referenced data based on specified flags.

## Definition

```c
void
QTNFree(QTNode *in)
```
## Detailed Description
QTNFree is a recursive function that properly deallocates a QTNode tree and all its associated resources. The function performs a post-order traversal, freeing child nodes before parent nodes to avoid accessing freed memory.

The function handles different types of memory deallocation based on flags:
- **Word data**: Frees the word string if QTN_WORDFREE flag is set and the node contains a value (QI_VAL)
- **Child arrays**: Frees the child pointer array for operator nodes after recursively freeing all children
- **ValNode**: Frees the referenced QueryItem if QTN_NEEDFREE flag is set
- **Node itself**: Always frees the QTNode structure

The function includes null pointer checking and stack depth verification to prevent issues during deep recursion.

## Parameters / Member Variables
- : Pointer to the QTNode tree root to be freed (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (stack overflow protection)
  - pfree (PostgreSQL memory deallocation)
  - QTNFree (recursive self-call)
- Data types and constants used:
  - QTNode
  - QI_VAL (query item type for values)
  - QI_OPR (query item type for operators)  
  - QTN_WORDFREE (flag indicating word should be freed)
  - QTN_NEEDFREE (flag indicating valnode should be freed)
- Called from (representative examples):
  - tsquery_and
  - tsquery_or
  - tsquery_not
  - CompareTSQ
  - findeq
  - tsquery_rewrite_query

## Notes and Other Information
- The function is null-safe and returns immediately if passed a NULL pointer
- Uses post-order traversal to ensure child nodes are freed before their parents
- Flag-based memory management allows selective deallocation of different components
- Essential for preventing memory leaks in tsquery processing operations
- Stack depth checking prevents stack overflow during deep recursion on large query trees