# tsvector_setweight_by_filter

## Location
[src/backend/utils/adt/tsvector_op.c:273-353](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L273-L353)

## Overview
PostgreSQL function that selectively sets the weight for specific lexemes in a TSVector based on a filter array of target lexeme names.

## Definition
```c
Datum tsvector_setweight_by_filter(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides fine-grained control over TSVector weights by allowing users to specify exactly which lexemes should have their weights modified. Unlike `tsvector_setweight` which affects all lexemes uniformly, this function only modifies lexemes that match entries in the provided filter array.

The function operates as follows:
1. Extracts the input TSVector, weight character, and lexeme filter array
2. Maps the weight character to numeric weight (A=3, B=2, C=1, D=0)
3. Creates a copy of the input TSVector to avoid modifying the original
4. Deconstructs the lexeme array to access individual text elements
5. For each lexeme in the filter array:
   - Performs binary search to locate the lexeme in the TSVector
   - If found and the lexeme has positional information, updates all position weights
6. Returns the selectively modified TSVector

This approach is optimized assuming the lexeme filter array is significantly shorter than the TSVector, making binary search the efficient strategy.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro
  - Argument 0: TSVector input (accessed via PG_GETARG_TSVECTOR(0))
  - Argument 1: Weight character A/a, B/b, C/c, D/d (accessed via PG_GETARG_CHAR(1))
  - Argument 2: Text array of lexemes to filter (accessed via PG_GETARG_ARRAYTYPE_P(2))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSVECTOR (macro for extracting TSVector argument)
  - PG_GETARG_CHAR (macro for extracting character argument)
  - PG_GETARG_ARRAYTYPE_P (macro for extracting array argument)
  - elog (PostgreSQL logging/error function)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation function)
  - memcpy (standard C memory copy function)
  - VARSIZE (macro for getting variable-length data size)
  - ARRPTR (macro for getting WordEntry array pointer)
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md) (PostgreSQL array deconstruction function)
  - VARDATA (macro for accessing variable-length data content)
  - VARHDRSZ (macro for variable-length header size)
  - [tsvector_bsearch](tsvector_bsearch.md) (binary search function for TSVector lexemes)
  - POSDATALEN (macro for getting position data length)
  - POSDATAPTR (macro for getting position data pointer)
  - WEP_SETWEIGHT (macro for setting weight in WordEntryPos)
  - PG_FREE_IF_COPY (macro for conditional memory cleanup)
  - PG_RETURN_POINTER (macro for returning pointer result)
- Called from (representative examples):
  - SQL function calls (accessible as setweight(tsvector, char, text[]))

## Notes and Other Information
- This is a PostgreSQL SQL-callable function (returns Datum)
- Supports selective weight assignment to specific lexemes only
- Uses binary search for efficient lexeme lookup in the TSVector
- Ignores NULL entries in the lexeme filter array
- Only modifies lexemes that have positional information (haspos=true)
- Weight mapping identical to tsvector_setweight: A/a→3, B/b→2, C/c→1, D/d→0
- Creates a new TSVector copy rather than modifying input in-place
- Optimized for cases where filter array is much smaller than the TSVector
- Essential for implementing targeted document relevance adjustments in full-text search