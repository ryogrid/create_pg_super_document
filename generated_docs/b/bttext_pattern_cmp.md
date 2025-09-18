# bttext_pattern_cmp

## Location
src/backend/utils/adt/varlena.c: 2883 - 2898

## Overview
A PostgreSQL B-tree comparison function that performs pattern-based comparison between two text values, returning an integer result suitable for B-tree indexing operations and LIKE clause support.

## Definition
```c
Datum bttext_pattern_cmp(PG_FUNCTION_ARGS)
```

## Detailed Description
The `bttext_pattern_cmp` function serves as a B-tree comparison support function for text pattern operations. It compares two text arguments using the same pattern-oriented comparison algorithm as other text pattern functions, but returns the raw integer comparison result rather than a boolean value. This makes it suitable for use as a B-tree operator class support function.

The function uses `internal_text_pattern_compare` to perform byte-wise comparison of the text data using `memcmp`. The returned integer follows standard comparison conventions: negative if arg1 < arg2, zero if arg1 == arg2, and positive if arg1 > arg2. This enables efficient B-tree operations for pattern-based text indexing.

## Parameters / Member Variables
- `arg1`: First text argument to compare (retrieved using `PG_GETARG_TEXT_PP(0)`)
- `arg2`: Second text argument to compare (retrieved using `PG_GETARG_TEXT_PP(1)`)

## Dependencies
- Functions called/Symbols referenced:
  - internal_text_pattern_compare
  - PG_GETARG_TEXT_PP
  - PG_FREE_IF_COPY
  - PG_RETURN_INT32
- Called from (representative examples):
  - No direct references found (likely used through PostgreSQL's B-tree operator class infrastructure)

## Notes and Other Information
- B-tree support function for text pattern comparison operations
- Returns integer comparison result (-1, 0, or +1) rather than boolean values
- Part of PostgreSQL's pattern comparison infrastructure designed for LIKE clause index support
- Uses memory management macros to handle potentially copied text arguments
- Located in `src/backend/utils/adt/varlena.c` at lines 2883-2898
- Compatible with "C" collation and designed for use in B-tree operator classes
- Essential for building efficient indexes that can support pattern matching operations