# tsquery_not

## Location
[src/backend/utils/adt/tsquery_op.c:159-188](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_op.c#L159-L188)

## Overview
Creates a negation query that matches documents where the specified tsquery does not match, implementing the NOT logical operator for text search queries.

## Definition

```c
Datum
tsquery_not(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function creates a NOT operation on a tsquery, which matches documents that do not contain the specified search terms or patterns. It constructs a new query tree with a NOT operator as the root node and the input query as its single child. The function handles empty queries by returning them unchanged, since negating an empty query would still be empty.

The implementation creates a new QTNode structure representing the NOT operator, sets up the necessary flags and metadata, and converts the input query into a child node. The resulting query tree is then converted back into a TSQuery format for return.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: The tsquery to be negated
## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSQUERY_COPY
  - [palloc0](../p/palloc0.md)
  - [QT2QTN](../Q/QT2QTN.md)
  - GETQUERY
  - GETOPERAND  
  - [QTN2QT](../Q/QTN2QT.md)
  - [QTNFree](../Q/QTNFree.md)
  - PG_FREE_IF_COPY
  - PG_RETURN_POINTER
- Data structures used:
  - TSQuery
  - [QTNode](../Q/QTNode.md)
  - QueryItem
  - QTN_NEEDFREE flag
  - QI_OPR type
  - OP_NOT operator

## Notes and Other Information
- Returns the original query unchanged if it's empty (size == 0)
- Creates a single-child query tree with NOT as the root operator
- Properly manages memory allocation and cleanup of intermediate structures
- Part of PostgreSQL's full-text search infrastructure
- Implements the  unary NOT operator in tsquery syntax
- The resulting query will match documents that do not satisfy the original query condition

## Simplified Source

```c
Datum
tsquery_not(PG_FUNCTION_ARGS)
{
    TSQuery input = PG_GETARG_TSQUERY_COPY(0);

    // Return empty query unchanged
    if (input->size == 0)
        return input;

    // Create NOT operator node
    QTNode *not_node = palloc0(sizeof(QTNode));
    not_node->flags |= QTN_NEEDFREE;

    // Set up NOT operator
    not_node->valnode = palloc0(sizeof(QueryItem));
    not_node->valnode->type = QI_OPR;
    not_node->valnode->qoperator.oper = OP_NOT;

    // Add input query as single child
    not_node->child = palloc0(sizeof(QTNode *));
    not_node->child[0] = QT2QTN(GETQUERY(input), GETOPERAND(input));
    not_node->nchild = 1;

    // Convert back to TSQuery and cleanup
    TSQuery result = QTN2QT(not_node);
    QTNFree(not_node);
    PG_FREE_IF_COPY(input, 0);

    PG_RETURN_POINTER(result);
}
```