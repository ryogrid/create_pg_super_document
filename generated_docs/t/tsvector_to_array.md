# tsvector_to_array

## Location
src/backend/utils/adt/tsvector_op.c: 720 - 746

## Overview
Converts a TSVector into a simple array of lexeme strings, extracting only the lexeme text without position or weight information.

## Definition
```c
Datum tsvector_to_array(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides a simple way to extract all lexemes from a TSVector as a PostgreSQL text array. It iterates through each lexeme entry in the TSVector and creates corresponding text elements, discarding any position or weight information that may be associated with the lexemes.

The function allocates a temporary array to hold Datum pointers for each lexeme, converts each lexeme from the internal C string format to PostgreSQL text format, then constructs a final PostgreSQL array containing all the lexemes. The resulting array maintains the same order as the lexemes appear in the TSVector (which is sorted alphabetically).

This is useful when you need just the vocabulary from a TSVector without the associated positional or weight metadata.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]`: Input TSVector to convert to lexeme array

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSVECTOR - Extract TSVector argument
  - ARRPTR - Get pointer to WordEntry array in TSVector
  - STRPTR - Get pointer to string data in TSVector  
  - cstring_to_text_with_len - Convert C string to PostgreSQL text
  - [construct_array_builtin](../c/construct_array_builtin.md) - Build PostgreSQL array from Datum elements
  - [palloc](../p/palloc.md) - Allocate memory
  - [pfree](../p/pfree.md) - Free allocated memory
  - PG_FREE_IF_COPY - Free copied arguments if needed
  - PG_RETURN_POINTER - Return result pointer
- Called from (representative examples):
  - No direct references found (likely called through SQL function dispatch)

## Notes and Other Information
- Returns only lexeme text, discarding position and weight information
- Maintains the alphabetical order of lexemes as stored in the TSVector
- Memory management includes cleanup of temporary element array and copied arguments
- Simpler alternative to tsvector_unnest when only lexeme text is needed
- Part of PostgreSQL's full-text search functionality for TSVector conversion
- Useful for extracting vocabulary lists from full-text search vectors