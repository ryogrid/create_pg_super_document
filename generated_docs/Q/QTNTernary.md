# QTNTernary

## Location
[src/backend/utils/adt/tsquery_util.c:201-249](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_util.c#L201-L249)

## Overview
QTNTernary is a recursive function that removes unnecessary intermediate nodes from a QTNode tree by flattening associative operations (AND/OR) to optimize query tree structure.

## Definition
```c
void QTNTernary(QTNode *in)
```

## Detailed Description
QTNTernary performs query tree optimization by eliminating redundant intermediate nodes in associative operations. For example, it transforms nested OR operations like "OR(a, OR(b, c))" into a flattened "OR(a, b, c)" structure. This optimization reduces tree depth and improves query processing efficiency.

The function works recursively, first processing all child nodes, then examining the current node. If the current node is an AND or OR operation, it looks for child nodes with the same operation type and flattens them by moving the grandchildren up one level, removing the intermediate node.

The function includes stack depth checking to prevent stack overflow during deep recursion and properly manages memory by freeing intermediate nodes that are no longer needed.

## Parameters / Member Variables
- `in`: Pointer to the QTNode tree to be optimized (modified in-place)

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md) (stack overflow protection)
  - [QTNTernary](QTNTernary.md) (recursive self-call)
  - [repalloc](../r/repalloc.md) (memory reallocation)
  - memmove, memcpy (memory operations)
  - [pfree](../p/pfree.md) (memory deallocation)
  - [QTNode](QTNode.md), QI_OPR, OP_AND, OP_OR, QTN_NEEDFREE (data types and constants)
- Called from (representative examples):
  - [tsquery_rewrite_query](../t/tsquery_rewrite_query.md) (in tsquery_rewrite.c)
  - [tsquery_rewrite](../t/tsquery_rewrite.md) (in tsquery_rewrite.c)

## Notes and Other Information
- Only flattens associative operations (AND, OR) as these are the only operations where flattening preserves semantic meaning
- Uses recursive approach with stack depth checking to handle arbitrarily deep trees safely
- Modifies the tree structure in-place, reallocating child arrays as needed
- Properly manages memory by freeing intermediate nodes marked with QTN_NEEDFREE
- Part of PostgreSQL's text search query optimization pipeline
- Located in src/backend/utils/adt/tsquery_util.c:201-249

## Simplified Source

```c
void
QTNTernary(QTNode *in)
{
    // Prevent stack overflow during recursion
    check_stack_depth();

    // Only process operator nodes
    if (in->valnode->type != QI_OPR)
        return;

    // Recursively optimize all children first
    for (int i = 0; i < in->nchild; i++)
        QTNTernary(in->child[i]);

    // Only flatten associative operations (AND, OR)
    if (in->valnode->qoperator.oper != OP_AND &&
        in->valnode->qoperator.oper != OP_OR)
        return;

    // Look for children with same operator type to flatten
    for (int i = 0; i < in->nchild; i++) {
        QTNode *child = in->child[i];

        // If child has same operator, flatten it
        if (child->valnode->type == QI_OPR &&
            in->valnode->qoperator.oper == child->valnode->qoperator.oper) {

            int old_nchild = in->nchild;

            // Expand parent's child array to accommodate grandchildren
            in->nchild += child->nchild - 1;
            in->child = (QTNode **) repalloc(in->child, in->nchild * sizeof(QTNode *));

            // Move remaining siblings to make room for grandchildren
            if (i + 1 != old_nchild)
                memmove(in->child + i + child->nchild, in->child + i + 1,
                        (old_nchild - i - 1) * sizeof(QTNode *));

            // Copy grandchildren into parent's child array
            memcpy(in->child + i, child->child, child->nchild * sizeof(QTNode *));
            i += child->nchild - 1;

            // Free the flattened intermediate node
            if (child->flags & QTN_NEEDFREE)
                pfree(child->valnode);
            pfree(child);
        }
    }
}
```