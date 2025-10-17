# tsquery_or

## Location
[src/backend/utils/adt/tsquery_op.c:84-113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_op.c#L84-L113)

## Overview
Implements the logical OR operation between two TSQuery objects, combining them to create a new TSQuery that matches documents containing terms from either input query.

## Definition

```c
Datum
tsquery_or(PG_FUNCTION_ARGS)
```
## Detailed Description
The `tsquery_or` function is a PostgreSQL built-in function that performs a logical OR operation on two TSQuery objects. It creates a new TSQuery that will match documents if they satisfy either of the input queries (or both). Like `tsquery_and`, it includes optimization logic: if either input query is empty (size == 0), it returns the other query since ORing with an empty query effectively means the non-empty query becomes the result. For non-empty queries, it uses the `join_tsqueries` helper function with the OP_OR operator to construct the combined query tree.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing two TSQuery inputs

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TSQUERY_COPY` - Extracts and copies TSQuery arguments
  - `PG_FREE_IF_COPY` - Frees memory if arguments were copied
  - `PG_RETURN_POINTER` - Returns pointer result for optimization cases
  - `PG_RETURN_TSQUERY` - Returns TSQuery result
  - [join_tsqueries](../j/join_tsqueries.md) - Helper function to combine queries with operator
  - [QTN2QT](../Q/QTN2QT.md) - Converts query tree node back to TSQuery format
  - `[QTNFree](../Q/QTNFree.md)` - Frees query tree node memory
  - `OP_OR` - OR operator constant
  - `TSQuery` - Text search query data type
  - `[QTNode](../Q/QTNode.md)` - [Query](../Q/Query.md) tree node structure

- Called from (representative examples):
  - No direct callers found in the analyzed codebase (likely called via SQL operator |)

## Notes and Other Information
- This function implements the PostgreSQL `|` operator for TSQuery objects
- Includes performance optimizations for empty queries
- Memory management is carefully handled with proper cleanup of intermediate structures
- The resulting query will match documents that satisfy either input condition
- Uses distance parameter of 0 since OR operations don't involve proximity
- Structurally very similar to `tsquery_and` but uses OP_OR instead of OP_AND

## Simplified Source

```c
Datum tsquery_or(PG_FUNCTION_ARGS)
{
    TSQuery a = PG_GETARG_TSQUERY_COPY(0);
    TSQuery b = PG_GETARG_TSQUERY_COPY(1);
    QTNode *result_node;
    TSQuery query;

    // Optimization: empty query OR anything = the other query
    if (a->size == 0) {
        PG_FREE_IF_COPY(a, 1);
        PG_RETURN_POINTER(b);
    } else if (b->size == 0) {
        PG_FREE_IF_COPY(b, 1);
        PG_RETURN_POINTER(a);
    }

    // Combine queries with OR operator
    result_node = join_tsqueries(a, b, OP_OR, 0);

    // Convert back to TSQuery format
    query = QTN2QT(result_node);

    // Cleanup memory
    QTNFree(result_node);
    PG_FREE_IF_COPY(a, 0);
    PG_FREE_IF_COPY(b, 1);

    PG_RETURN_TSQUERY(query);
}
```