# tsquerytree

## Location
src/backend/utils/adt/tsquery.c: 1363 - 1399

## Overview
A debug function that converts a TSQuery structure into a human-readable infix notation string representation for visualization purposes.

## Definition
```c
Datum tsquerytree(PG_FUNCTION_ARGS)
```

## Detailed Description
The `tsquerytree` function is a PostgreSQL utility function designed for debugging and visualization of TSQuery structures. It converts the internal prefix notation representation of a full-text search query into a more readable infix notation string format.

The function performs the following operations:
1. Handles empty queries by returning an empty text result
2. Cleans NOT operations from the query using `clean_NOT` helper function
3. If the cleaned query is null (indicating a trivial true condition), returns "T"
4. Otherwise, uses an INFIX structure to build the infix representation:
   - Initializes a buffer with 32 characters initial capacity
   - Uses the `infix` helper function to recursively convert the query tree
   - Returns the resulting string as PostgreSQL text

This function is particularly useful for understanding query structure in non-leaf pages of indexes and for debugging complex full-text search queries.

## Parameters / Member Variables
This function uses PostgreSQL's function call convention:
- Uses `PG_GETARG_TSQUERY(0)` to retrieve the TSQuery input parameter
- Returns a text representation via `PG_RETURN_TEXT_P()`

Internal variables:
- `nrm`: INFIX structure for building the string representation
- `q`: Cleaned QueryItem array after NOT processing
- `len`: Length of the cleaned query
- `res`: Final text result

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSQUERY: Extract TSQuery from function arguments
  - GETQUERY: Get query items array from TSQuery
  - GETOPERAND: Get operand string data from TSQuery
  - clean_NOT: Remove/clean NOT operations from query
  - palloc: Allocate memory for buffer and INFIX structure
  - pfree: Free allocated memory
  - cstring_to_text: Convert C string to PostgreSQL text
  - cstring_to_text_with_len: Convert C string with specific length to text
  - infix: Convert query tree to infix notation
  - SET_VARSIZE: Set variable-length data size
  - PG_FREE_IF_COPY: Free input if it's a copy
  - PG_RETURN_TEXT_P: Return text result

- Called from (representative examples):
  - No direct references found in codebase (likely called via PostgreSQL's type system)

## Notes and Other Information
- This is primarily a debug/utility function rather than a core operational function
- Designed for visualization of queries executed in non-leaf pages of indexes
- The comment indicates it's specifically for viewing queries in index structures
- Uses dynamic buffer allocation that grows as needed during string construction
- The function handles edge cases like empty queries and trivial conditions gracefully
- Located in src/backend/utils/adt/tsquery.c:1363-1399
- Useful for developers working with full-text search indexes and query optimization