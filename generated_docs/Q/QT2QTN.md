# QT2QTN

## Location
[src/backend/utils/adt/tsquery_util.c:25-63](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_util.c#L25-L63)

## Overview
Builds a QTNode tree structure for a tsquery given in QueryItem array format, providing a recursive tree representation for text search query processing.

## Definition

```c
QTNode *
QT2QTN(QueryItem *in, char *operand)
```
## Detailed Description
QT2QTN is a recursive function that converts a flat QueryItem array representation of a text search query into a hierarchical QTNode tree structure. This transformation is essential for tsquery processing, as it creates a tree that can be efficiently traversed and manipulated for various text search operations.

The function handles two types of query items:
1. **Operators (QI_OPR)**: Creates internal nodes with child pointers, handling both unary (NOT) and binary (AND, OR) operators
2. **Operands**: Creates leaf nodes containing the actual search terms with their signature bits

For operators, the function recursively processes child nodes and combines their signature bits using bitwise OR. The signature bits are used for efficient query optimization and filtering.

## Parameters / Member Variables
- `*in`: Pointer to the current QueryItem in the array being processed
- `*operand`: Pointer to the operand string data, used to set word pointers for leaf nodes
## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (memory allocation)
  - [check_stack_depth](../c/check_stack_depth.md) (stack overflow protection)
  - [QT2QTN](QT2QTN.md) (recursive self-call)
- Data types used:
  - QueryItem
  - [QTNode](QTNode.md)
  - QI_OPR (query item type constant)
  - OP_NOT (operator type constant)
- Called from (representative examples):
  - [join_tsqueries](../j/join_tsqueries.md)
  - [tsquery_not](../t/tsquery_not.md)
  - [CompareTSQ](../C/CompareTSQ.md)
  - [tsquery_rewrite_query](../t/tsquery_rewrite_query.md)
  - [tsquery_rewrite](../t/tsquery_rewrite.md)

## Notes and Other Information
- The function includes stack depth checking to prevent stack overflow during deep recursion
- Signature bits are calculated as  for efficient bitwise operations
- Memory is allocated using palloc0 to ensure zero-initialized structures
- The function is fundamental to PostgreSQL's full-text search functionality, converting linear query representations into tree structures suitable for query execution and optimization

## Simplified Source

```c
QTNode *
QT2QTN(QueryItem *in, char *operand)
{
    // Allocate and initialize new node
    QTNode *node = (QTNode *) palloc0(sizeof(QTNode));

    // Prevent stack overflow during recursion
    check_stack_depth();

    node->valnode = in;

    if (in->type == QI_OPR) {
        // Handle operator nodes - allocate space for children
        node->child = (QTNode **) palloc0(sizeof(QTNode *) * 2);

        // Process left child recursively
        node->child[0] = QT2QTN(in + 1, operand);
        node->sign = node->child[0]->sign;

        if (in->qoperator.oper == OP_NOT) {
            // Unary NOT operator - only one child
            node->nchild = 1;
        } else {
            // Binary operator (AND/OR) - process right child
            node->nchild = 2;
            node->child[1] = QT2QTN(in + in->qoperator.left, operand);
            node->sign |= node->child[1]->sign;  // Combine signatures
        }
    } else if (operand) {
        // Handle operand leaf nodes
        node->word = operand + in->qoperand.distance;
        node->sign = ((uint32) 1) << (in->qoperand.valcrc % 32);  // Calculate signature bit
    }

    return node;
}
```