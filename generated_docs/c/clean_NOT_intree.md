# clean_NOT_intree

## Location
src/backend/utils/adt/tsquery_cleanup.c: 136 - 189

## Overview
The `clean_NOT_intree` function removes and simplifies NOT operators from a binary tree representation of a TSQuery, optimizing the query structure by eliminating negation operations that always evaluate to TRUE.

## Definition
```c
static NODE *clean_NOT_intree(NODE *node)
```

## Detailed Description
The `clean_NOT_intree` function performs recursive optimization of a TSQuery tree by removing NOT operators and simplifying the resulting structure. Since NOT operators in text search contexts typically return TRUE (as noted in the comments), they can be eliminated to optimize query execution.

The function operates through recursive tree traversal with different logic based on node types:

1. **Value nodes (QI_VAL)**: Returned unchanged as they contain no operators to optimize
2. **NOT operators (OP_NOT)**: Completely removed by freeing the entire subtree and returning NULL
3. **OR operators (OP_OR)**: If either child becomes NULL after recursive cleaning, the entire OR subtree is removed since OR with a NULL operand cannot be meaningful
4. **AND/PHRASE operators**: More complex handling where NULL children are removed but the operation can continue with remaining non-NULL children. If both children become NULL, the node is removed; if one child is NULL, the node is replaced with the remaining child.

The function includes stack overflow protection and uses careful memory management to prevent leaks during tree restructuring.

## Parameters / Member Variables
- `node`: Pointer to the current NODE in the binary tree being cleaned; represents the root of the subtree to be optimized

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (stack overflow protection)
  - [freetree](../f/freetree.md) (memory deallocation for removed subtrees)
  - [clean_NOT_intree](clean_NOT_intree.md) (recursive self-call)
  - [pfree](../p/pfree.md) (individual node deallocation)
  - Assert (debugging assertion)
- Called from (representative examples):
  - [clean_NOT_intree](clean_NOT_intree.md) (recursive calls)
  - [clean_NOT](clean_NOT.md)

## Notes and Other Information
- This function is part of PostgreSQL's text search query cleanup and optimization system
- The optimization is based on the principle that NOT operators in full-text search typically always return TRUE
- Different operator types receive different optimization strategies based on their logical properties
- The function can significantly reduce query tree complexity by removing redundant negation operations
- Memory management is critical as the function restructures the tree and must properly deallocate removed nodes
- The resulting optimized tree maintains the same logical meaning while being more efficient to execute