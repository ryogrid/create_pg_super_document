# bytealt

## Location
src/backend/utils/adt/varlena.c: 3858 - 3877

## Overview
A PostgreSQL function that implements the less-than operator (<) for bytea (binary string) values, performing lexicographic comparison.

## Definition
```c
Datum bytealt(PG_FUNCTION_ARGS)
```

## Detailed Description
The `bytealt` function implements the less-than comparison operator for bytea data type in PostgreSQL. It performs lexicographic (dictionary-style) comparison between two binary string values. The function compares bytes sequentially using `memcmp()` up to the length of the shorter string. If the byte comparison yields a definitive result (one string is lexicographically smaller), that result is returned. If the compared portions are identical, the shorter string is considered less than the longer string.

Unlike the equality/inequality functions, this function doesn't include the length optimization since lexicographic comparison requires examining the actual byte content regardless of length differences.

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
  - Used as the less-than operator function for bytea type in SQL operations
  - Referenced by the PostgreSQL type system for bytea ordering operations

## Notes and Other Information
- Implements lexicographic (dictionary-style) ordering for binary data
- Returns true if arg1 < arg2 in lexicographic order
- Shorter strings are considered less than longer strings when the compared portions are identical
- Part of the bytea comparison function family in varlena.c
- Memory-safe implementation with proper cleanup
- Located in src/backend/utils/adt/varlena.c:3858-3877