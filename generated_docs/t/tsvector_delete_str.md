# tsvector_delete_str

## Location
[src/backend/utils/adt/tsvector_op.c:554-577](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L554-L577)

## Overview
Deletes a specified lexeme from a TSVector, implementing the user-level ts_delete(tsvector, text) function.

## Definition

```c
struct_array_builtin(lexemes, TEXTOID, &dlexemes, &nulls, &nlex);
```
## Detailed Description
This function removes a single lexeme (specified as a text string) from a TSVector. It performs a binary search to locate the lexeme within the TSVector's sorted lexeme array. If the lexeme is found, it creates a new TSVector with the lexeme removed using the  helper function. If the lexeme is not present in the TSVector, the original TSVector is returned unchanged.

The function handles PostgreSQL's variable-length data structures properly, extracting the lexeme text and its length from the input text parameter. Memory management is handled through PostgreSQL's standard mechanisms with appropriate cleanup of copied arguments.

## Parameters / Member Variables
- : Input TSVector from which to delete the lexeme
- : Text parameter containing the lexeme string to delete

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSVECTOR - Extract TSVector argument
  - PG_GETARG_TEXT_PP - Extract text argument  
  - VARDATA_ANY - Get pointer to variable-length data
  - VARSIZE_ANY_EXHDR - Get size of variable-length data excluding header
  - [tsvector_bsearch](tsvector_bsearch.md) - Binary search for lexeme in TSVector
  - [tsvector_delete_by_indices](tsvector_delete_by_indices.md) - Helper to delete lexemes by index array
  - PG_FREE_IF_COPY - Free copied arguments if needed
  - PG_RETURN_POINTER - Return result pointer
- Called from (representative examples):
  - No direct references found (likely called through SQL function dispatch)

## Notes and Other Information
- Returns the original TSVector unchanged if the specified lexeme is not found
- Uses binary search for efficient lexeme lookup in the sorted TSVector structure  
- Properly handles PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) for variable-length data
- Memory management follows PostgreSQL conventions with appropriate cleanup of potentially copied input arguments
- Part of PostgreSQL's full-text search functionality for TSVector manipulation