# plainnode

## Location
[src/backend/utils/adt/tsquery_cleanup.c:62-96](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_cleanup.c#L62-L96)

## Overview
The `plainnode` function converts a binary tree representation of a TSQuery back into a flat array representation, performing a depth-first traversal while maintaining correct operator precedence and positioning information.

## Definition
```c
static void plainnode(PLAINTREE *state, NODE *node)
```

## Detailed Description
The `plainnode` function is a recursive function that performs the inverse operation of `maketree`. It traverses a binary tree representation of a TSQuery and converts it back into a linearized array format stored in a PLAINTREE structure. This conversion is essential for serializing the tree back into the compact QueryItem array format used by PostgreSQL's text search system.

The function uses depth-first traversal and handles different node types appropriately:
- For value nodes (QI_VAL), it simply copies the node and advances the position
- For NOT operators, it sets the left field to 1 and recursively processes only the right subtree
- For binary operators (AND, OR), it processes the right subtree first, then calculates the left field offset, and finally processes the left subtree

The function dynamically resizes the output array when needed using `repalloc`, and includes stack overflow protection. It also frees each processed node to prevent memory leaks.

## Parameters / Member Variables
- `state`: Pointer to a PLAINTREE structure that maintains the current state of the conversion, including the output array, current position, and array length
- `node`: Pointer to the current NODE in the binary tree being converted

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (stack overflow protection)
  - [repalloc](../r/repalloc.md) (memory reallocation)
  - memcpy (memory copying)
  - [plainnode](plainnode.md) (recursive self-call)
  - [pfree](pfree.md) (memory deallocation)
- Called from (representative examples):
  - [plainnode](plainnode.md) (recursive calls)
  - [plaintree](plaintree.md)

## Notes and Other Information
- This function is part of PostgreSQL's text search query cleanup and optimization system
- The function performs the inverse operation of `maketree`, converting trees back to flat arrays
- Memory management includes both dynamic resizing of the output array and cleanup of processed nodes
- The traversal order and offset calculations ensure that the resulting flat array maintains the correct structure for query evaluation
- Stack depth checking prevents potential issues with deeply nested query trees