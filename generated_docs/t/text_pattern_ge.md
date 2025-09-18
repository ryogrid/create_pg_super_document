# text_pattern_ge

## Location
src/backend/utils/adt/varlena.c: 2851 - 2866

## Overview
A PostgreSQL function that performs pattern-based greater-than-or-equal comparison between two text values, supporting character-by-character comparison suitable for building indexes used with LIKE clauses.

## Definition


## Detailed Description
The  function compares two text arguments using a pattern-oriented comparison algorithm and returns true if the first argument is greater than or equal to the second argument. This function is specifically designed to support character-by-character comparison of text datums, enabling the construction of indexes that are suitable for LIKE clause operations.

The function uses  to perform the actual comparison, which conducts a byte-wise comparison using  on the variable-length text data. The comparison is compatible with regular text equality/inequality operators and support functions when using "C" collation.

## Parameters / Member Variables
- : First text argument to compare (retrieved using )
- : Second text argument to compare (retrieved using )

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
- Returns a boolean result (>= 0 from the comparison function translates to true)
- Located in  at lines 2851-2866
- Compatible with "C" collation text comparison operators