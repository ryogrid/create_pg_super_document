# QTNCopy

## Location
src/backend/utils/adt/tsquery_util.c: 396 - 433

## Overview
Creates a deep copy of a QTNode tree structure with all modifiable copies of words and value nodes.

## Definition
```c
QTNode *QTNCopy(QTNode *in)
```

## Detailed Description
QTNCopy performs a recursive deep copy of a QTNode tree, creating new memory allocations for all components including the node structure, value nodes, and word strings. The function handles both leaf nodes (QI_VAL type with word data) and internal nodes (with child arrays) appropriately. It sets memory management flags (QTN_NEEDFREE, QTN_WORDFREE) to ensure proper cleanup of the allocated resources. Stack depth checking prevents infinite recursion scenarios.

## Parameters / Member Variables
- `in`: Source QTNode tree structure to be copied

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (prevents stack overflow during recursion)
  - palloc (allocates memory for nodes and data)
  - memcpy (copies word string data)
  - QTNCopy (recursive call for child nodes)
  - QTN_NEEDFREE (flag indicating node needs memory cleanup)
  - QTN_WORDFREE (flag indicating word needs memory cleanup)
  - QI_VAL (query item type for value nodes)
- Called from (representative examples):
  - findeq (query rewriting operations)
  - QTNCopy (recursive self-calls)

## Notes and Other Information
- Recursively copies the entire tree structure to ensure complete independence from original
- Properly null-terminates copied word strings for QI_VAL type nodes
- Sets appropriate memory management flags to track allocated resources
- Uses stack depth checking to prevent stack overflow in deeply nested trees
- Essential for query rewriting operations where original trees must be preserved