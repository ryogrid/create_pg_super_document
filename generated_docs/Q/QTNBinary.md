# QTNBinary

## Location
[src/backend/utils/adt/tsquery_util.c:250-291](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_util.c#L250-L291)

## Overview
QTNBinary is a recursive function that converts a QTNode tree to binary tree structure by inserting intermediate nodes, serving as the opposite operation to QTNTernary.

## Definition
```c
void QTNBinary(QTNode *in)
```

## Detailed Description
QTNBinary transforms multi-child operator nodes into strictly binary trees by creating intermediate nodes. For nodes with more than 2 children, it repeatedly creates new intermediate nodes that combine the first two children, then replaces the original children with the new intermediate node and the last child, continuing until only 2 children remain.

The function works recursively, first converting all child subtrees to binary form, then processing the current node. Each newly created intermediate node inherits the same operator type as the parent and has its signature computed as the bitwise OR of its children's signatures.

This binary tree structure is often required for certain query processing algorithms or optimization phases that expect strictly binary trees.

## Parameters / Member Variables
- `in`: Pointer to the QTNode tree to be converted to binary form (modified in-place)

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (stack overflow protection)
  - QTNBinary (recursive self-call)
  - palloc0 (zero-initialized memory allocation)
  - QTNode, QueryItem, QI_OPR, QTN_NEEDFREE (data types and constants)
- Called from (representative examples):
  - tsquery_rewrite_query (in tsquery_rewrite.c)
  - tsquery_rewrite (in tsquery_rewrite.c)

## Notes and Other Information
- Only processes operator nodes (QI_OPR type), leaving leaf nodes unchanged
- Creates intermediate nodes with QTN_NEEDFREE flag to ensure proper memory management
- Newly created intermediate nodes inherit the operator type from their parent
- Computes signatures for new intermediate nodes using bitwise OR of children signatures
- Uses recursive approach with stack depth checking for safety
- Modifies tree structure in-place, reducing the number of children per node to at most 2
- Part of PostgreSQL's text search query processing pipeline
- Located in src/backend/utils/adt/tsquery_util.c:250-291