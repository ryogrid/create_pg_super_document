# byteagt

## Location
[src/backend/utils/adt/varlena.c:3898-3917](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L3898-L3917)

## Overview
A PostgreSQL function that implements the greater-than operator (>) for bytea (binary string) values, performing lexicographic comparison.

## Definition
```c
Datum byteagt(PG_FUNCTION_ARGS)
```

## Detailed Description
The `byteagt` function implements the greater-than comparison operator for bytea data type in PostgreSQL. It performs lexicographic (dictionary-style) comparison between two binary string values, returning true if the first argument is greater than the second. The function compares bytes sequentially using `memcmp()` up to the length of the shorter string. If the byte comparison shows the first string is lexicographically larger, it returns true. If the compared portions are identical, it returns true if the first string is longer than the second string.

The logic is the complement of `bytealt`, using greater-than comparison (cmp > 0) and length comparison (len1 > len2) instead of their less-than equivalents.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides:
  - `arg1`: First bytea value (as bytea pointer)
  - `arg2`: Second bytea value (as bytea pointer)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_BYTEA_PP`: Gets bytea argument with detoasting
  - `VARSIZE_ANY_EXHDR`: Macro to get variable-length data size excluding header
  - `VARDATA_ANY`: Macro to get variable-length data portion
  - `memcmp`: Standard C library function for memory comparison
  - `Min`: Macro to find minimum of two values
  - `PG_FREE_IF_COPY`: Macro to free memory if value was copied during detoasting
  - `PG_RETURN_BOOL`: Macro to return boolean result

- Called from (representative examples):
  - Used as the greater-than operator function for bytea type in SQL operations
  - Referenced by the PostgreSQL type system for bytea ordering operations

## Notes and Other Information
- Implements lexicographic (dictionary-style) ordering for binary data
- Returns true if arg1 > arg2 in lexicographic order
- Longer strings are considered greater than shorter strings when the compared portions are identical
- Part of the bytea comparison function family in varlena.c
- Memory-safe implementation with proper cleanup
- Located in src/backend/utils/adt/varlena.c:3898-3917