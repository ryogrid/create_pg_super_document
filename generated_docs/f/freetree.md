# freetree

## Location
src/backend/utils/adt/tsquery_cleanup.c: 115 - 135

## Overview
The `freetree` function recursively deallocates memory for an entire binary tree of NODE structures used in PostgreSQL's text search query processing.

## Definition
```c
static void freetree(NODE *node)
```

## Detailed Description
The `freetree` function performs a post-order traversal of a binary tree to safely deallocate all NODE structures and their associated memory. This is a critical memory management function that ensures proper cleanup of tree structures created during text search query processing.

The function uses a recursive post-order traversal strategy: it first recursively frees the left subtree, then the right subtree, and finally frees the current node. This order ensures that child nodes are freed before their parent, preventing memory access violations and ensuring complete cleanup.

The function includes safety checks for NULL pointers and stack overflow protection to handle potentially deep recursion in large query trees. This makes it robust against malformed input or extremely complex queries.

## Parameters / Member Variables
- `node`: Pointer to the root NODE of the binary tree to be deallocated; can be NULL (function handles this gracefully)

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (stack overflow protection)
  - freetree (recursive self-call for subtrees)
  - pfree (PostgreSQL memory deallocation)
- Called from (representative examples):
  - clean_NOT_intree
  - clean_stopword_intree

## Notes and Other Information
- This function is part of PostgreSQL's text search query cleanup and optimization system
- Uses post-order traversal to ensure safe memory deallocation
- Includes NULL pointer checking for defensive programming
- Stack depth checking prevents potential issues with deeply nested query trees
- Essential for preventing memory leaks in text search query processing
- The function is designed to be safe to call on any NODE tree, including NULL or partially constructed trees