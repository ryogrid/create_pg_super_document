# tsquery_phrase_distance

## Location
[src/backend/utils/adt/tsquery_op.c:114-149](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery_op.c#L114-L149)

## Overview
Implements the phrase search operation between two TSQuery objects with a specified distance constraint, creating a new TSQuery that matches documents where terms appear within the given proximity.

## Definition

```c
Datum
tsquery_phrase_distance(PG_FUNCTION_ARGS)
```
## Detailed Description
The `tsquery_phrase_distance` function is a PostgreSQL built-in function that performs a phrase operation on two TSQuery objects with a user-specified maximum distance between terms. It creates a new TSQuery that will match documents only if the terms from both input queries appear within the specified distance of each other in the document. The function includes strict validation of the distance parameter (must be between 0 and MAXENTRYPOS) and the same empty-query optimizations as the AND/OR functions. It uses the `join_tsqueries` helper function with the OP_PHRASE operator and passes the distance parameter to control proximity matching.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_TSQUERY_COPY` - Extracts and copies TSQuery arguments
  - `PG_GETARG_INT32` - Extracts integer distance argument
  - `PG_FREE_IF_COPY` - Frees memory if arguments were copied
  - `PG_RETURN_POINTER` - Returns pointer result for optimization cases
  - `PG_RETURN_TSQUERY` - Returns TSQuery result
  - `ereport` - Reports errors for invalid distance values
  - [join_tsqueries](../j/join_tsqueries.md) - Helper function to combine queries with phrase operator
  - [QTN2QT](../Q/QTN2QT.md) - Converts query tree node back to TSQuery format
  - `[QTNFree](../Q/QTNFree.md)` - Frees query tree node memory
  - `OP_PHRASE` - Phrase operator constant
  - `MAXENTRYPOS` - Maximum allowed position/distance value
  - `TSQuery` - Text search query data type
  - `[QTNode](../Q/QTNode.md)` - [Query](../Q/Query.md) tree node structure

- Called from (representative examples):
  - [tsquery_phrase](tsquery_phrase.md) - Default phrase search with distance 1

## Notes and Other Information
- This function implements the PostgreSQL phrase search with custom distance
- Validates that distance is within valid range [0, MAXENTRYPOS] before proceeding
- Distance of 0 means terms must be adjacent, higher values allow more separation
- Includes performance optimizations for empty queries
- Memory management is carefully handled with proper cleanup of intermediate structures
- Used as the underlying implementation for other phrase search functions
- The resulting query enforces both term presence and proximity constraints