# tsquery_numnode

## Location
src/backend/utils/adt/tsquery_op.c: 23 - 32

## Overview
Returns the number of lexemes and operators in a text search query (TSQuery), providing a count of all nodes in the query tree structure.

## Definition


## Detailed Description
The  function is a PostgreSQL built-in function that calculates and returns the total number of nodes in a TSQuery object. It accesses the  field of the TSQuery structure, which contains the count of all lexemes (search terms) and operators (AND, OR, NOT, phrase operators) that make up the query tree. This function provides insight into the complexity of a text search query.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that contains the TSQuery input

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts TSQuery from function arguments
  -  - Frees memory if argument was copied
  -  - Returns 32-bit integer result
  -  - Text search query data type
  -  - Query tree node structure

- Called from (representative examples):
  - No direct callers found in the analyzed codebase

## Notes and Other Information
- This function is typically used for analyzing query complexity or debugging purposes
- The size field represents the total count of both leaf nodes (lexemes) and internal nodes (operators)
- Memory management is handled automatically through PG_FREE_IF_COPY
- Returns a simple integer count, making it useful for query optimization decisions