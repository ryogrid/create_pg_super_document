# tsvector_setweight

## Location
[src/backend/utils/adt/tsvector_op.c:211-272](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L211-L272)

## Overview
PostgreSQL function that sets the weight for all positional entries in a TSVector to a specified weight class (A, B, C, or D).

## Definition
```c
Datum tsvector_setweight(PG_FUNCTION_ARGS)
```

## Detailed Description
This function modifies a TSVector by setting all position entries to have the same weight. In PostgreSQL full-text search, weights are used to indicate the relative importance of terms, with A being the highest weight and D being the lowest. The function:

1. Accepts a TSVector and a character indicating the desired weight (A/a, B/b, C/c, D/d)
2. Maps the character to the corresponding numeric weight value (A=3, B=2, C=1, D=0)
3. Creates a copy of the input TSVector to avoid modifying the original
4. Iterates through all word entries that have positional information
5. Updates each position entry to use the specified weight
6. Returns the modified TSVector

This operation is commonly used to assign uniform importance to all terms in a document or to downgrade/upgrade the significance of all terms simultaneously.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro
  - Argument 0: TSVector input (accessed via PG_GETARG_TSVECTOR(0))
  - Argument 1: Weight character A/a, B/b, C/c, D/d (accessed via PG_GETARG_CHAR(1))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSVECTOR (macro for extracting TSVector argument)
  - PG_GETARG_CHAR (macro for extracting character argument)
  - elog (PostgreSQL logging/error function)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation function)
  - memcpy (standard C memory copy function)
  - VARSIZE (macro for getting variable-length data size)
  - ARRPTR (macro for getting WordEntry array pointer)
  - POSDATALEN (macro for getting position data length)
  - POSDATAPTR (macro for getting position data pointer)
  - WEP_SETWEIGHT (macro for setting weight in WordEntryPos)
  - PG_FREE_IF_COPY (macro for conditional memory cleanup)
  - PG_RETURN_POINTER (macro for returning pointer result)
- Called from (representative examples):
  - SQL function calls (accessible as setweight(tsvector, char))

## Notes and Other Information
- This is a PostgreSQL SQL-callable function (returns Datum)
- Weight mapping: A/a→3, B/b→2, C/c→1, D/d→0 (higher numeric values = higher importance)
- Only affects TSVector entries that have positional information (haspos=true)
- Creates a new TSVector copy rather than modifying the input in-place
- Throws an ERROR for invalid weight characters
- Case-insensitive weight specification (both uppercase and lowercase accepted)
- Essential for implementing document ranking and relevance scoring in full-text search