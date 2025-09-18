# text_pattern_gt

## Location
[src/backend/utils/adt/varlena.c:2867-2882](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L2867-L2882)

## Overview
A PostgreSQL function that performs pattern-based greater-than comparison between two text values, supporting character-by-character comparison suitable for building indexes used with LIKE clauses.

## Definition
```c
Datum text_pattern_gt(PG_FUNCTION_ARGS)
```

## Detailed Description
The `text_pattern_gt` function compares two text arguments using a pattern-oriented comparison algorithm and returns true if the first argument is strictly greater than the second argument. This function is specifically designed to support character-by-character comparison of text datums, enabling the construction of indexes that are suitable for LIKE clause operations.

The function uses `internal_text_pattern_compare` to perform the actual comparison, which conducts a byte-wise comparison using `memcmp` on the variable-length text data. The comparison is compatible with regular text equality/inequality operators and support functions when using "C" collation.

## Parameters / Member Variables
- `arg1`: First text argument to compare (retrieved using `PG_GETARG_TEXT_PP(0)`)
- `arg2`: Second text argument to compare (retrieved using `PG_GETARG_TEXT_PP(1)`)

## Dependencies
- Functions called/Symbols referenced:
  - [internal_text_pattern_compare](../i/internal_text_pattern_compare.md)
  - PG_GETARG_TEXT_PP
  - PG_FREE_IF_COPY
  - PG_RETURN_BOOL
- Called from (representative examples):
  - No direct references found (likely used through PostgreSQL's function call infrastructure)

## Notes and Other Information
- Part of PostgreSQL's pattern comparison operator family designed for LIKE clause index support
- Uses memory management macros to handle potentially copied text arguments
- Returns a boolean result (> 0 from the comparison function translates to true)
- Located in `src/backend/utils/adt/varlena.c` at lines 2867-2882
- Compatible with "C" collation text comparison operators
- Similar to `text_pattern_ge` but uses strict greater-than comparison (> 0) instead of greater-than-or-equal (>= 0)