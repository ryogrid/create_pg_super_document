# tsvector_length

## Location
src/backend/utils/adt/tsvector_op.c: 201 - 210

## Overview
PostgreSQL function that returns the number of distinct lexemes (words) contained in a TSVector.

## Definition
```c
Datum tsvector_length(PG_FUNCTION_ARGS)
```

## Detailed Description
This simple utility function provides a way to determine the cardinality of a TSVector by returning the count of distinct lexemes it contains. The function directly accesses the `size` field of the TSVector structure, which stores the number of unique words/terms in the vector. This is a read-only operation that does not modify the input TSVector.

The function is straightforward:
1. Extracts the TSVector from the function arguments
2. Reads the size field which contains the lexeme count
3. Cleans up memory if necessary
4. Returns the count as a 32-bit integer

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro
  - Argument 0: TSVector input (accessed via PG_GETARG_TSVECTOR(0))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSVECTOR (macro for extracting TSVector argument)
  - PG_FREE_IF_COPY (macro for conditional memory cleanup)
  - PG_RETURN_INT32 (macro for returning 32-bit integer result)
- Called from (representative examples):
  - SQL function calls (accessible as length(tsvector))

## Notes and Other Information
- This is a PostgreSQL SQL-callable function (returns Datum)
- Returns the number of unique lexemes, not the total word count with positions
- Very efficient operation as it only reads a single integer field
- The returned value corresponds to the number of distinct terms indexed in the TSVector
- Commonly used for statistics and query planning in text search operations
- Does not count position entries - only unique lexemes