# tsquery_and

## Location
[src/backend/utils/adt/tsquery_op.c:54-83](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_op.c#L54-L83)

## Overview
Implements the logical AND operation between two TSQuery objects, combining them to create a new TSQuery that matches documents containing terms from both input queries.

## Definition

```c
Datum
tsquery_and(PG_FUNCTION_ARGS)
```
## Detailed Description
The `tsquery_and` function is a PostgreSQL built-in function that performs a logical AND operation on two TSQuery objects. It creates a new TSQuery that will match documents only if they satisfy both input queries. The function includes optimization logic: if either input query is empty (size == 0), it returns the other query since ANDing with an empty query effectively means the non-empty query becomes the result. For non-empty queries, it uses the `join_tsqueries` helper function with the OP_AND operator to construct the combined query tree.

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
  - `OP_AND` - AND operator constant
  - `TSQuery` - Text search query data type
  - `[QTNode](../Q/QTNode.md)` - [Query](../Q/Query.md) tree node structure

- Called from (representative examples):
  - No direct callers found in the analyzed codebase (likely called via SQL operator &)

## Notes and Other Information
- This function implements the PostgreSQL `&` operator for TSQuery objects
- Includes performance optimizations for empty queries
- Memory management is carefully handled with proper cleanup of intermediate structures
- The resulting query will match documents that satisfy both input conditions
- Uses distance parameter of 0 since AND operations don't involve proximity