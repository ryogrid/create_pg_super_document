# tsvector_strip

## Location
src/backend/utils/adt/tsvector_op.c: 168 - 200

## Overview
PostgreSQL function that removes position and weight information from a TSVector, keeping only the lexemes and their lengths.

## Definition
```c
Datum tsvector_strip(PG_FUNCTION_ARGS)
```

## Detailed Description
This function creates a new TSVector that contains only the lexemes (words) from the input TSVector, stripping away all positional information and weights. The resulting TSVector is more compact as it eliminates the position data that can take up significant space. This operation is useful when you only need to know which terms are present in a document without caring about their positions or importance weights.

The function:
1. Calculates the required size for the stripped TSVector (lexemes only)
2. Allocates memory for the new TSVector structure  
3. Copies each lexeme string while setting haspos=0 for all entries
4. Updates word entry positions to reflect the new compact layout
5. Returns the stripped TSVector

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro
  - Argument 0: TSVector input to strip (accessed via PG_GETARG_TSVECTOR(0))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSVECTOR (macro for extracting TSVector argument)
  - ARRPTR (macro for getting WordEntry array pointer)
  - CALCDATASIZE (macro for calculating required data size)
  - palloc0 (PostgreSQL memory allocation function)
  - SET_VARSIZE (macro for setting variable-length data size)
  - STRPTR (macro for getting string data pointer)
  - memcpy (standard C memory copy function)
  - PG_FREE_IF_COPY (macro for conditional memory cleanup)
  - PG_RETURN_POINTER (macro for returning pointer result)
- Called from (representative examples):
  - SQL function calls (accessible as strip(tsvector))

## Notes and Other Information
- This is a PostgreSQL SQL-callable function (returns Datum)
- The output TSVector will always have haspos=0 for all word entries
- Memory usage is reduced since position data arrays are eliminated
- The original lexeme ordering and content is preserved
- Useful for storage optimization when positional information is not needed
- The function handles memory management properly with PG_FREE_IF_COPY