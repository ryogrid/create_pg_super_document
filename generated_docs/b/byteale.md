# byteale

## Location
[src/backend/utils/adt/varlena.c:3878-3897](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L3878-L3897)

## Overview
A PostgreSQL function that implements the less-than-or-equal-to operator (<=) for bytea (binary string) values, performing lexicographic comparison.

## Definition
```c
Datum byteale(PG_FUNCTION_ARGS)
```

## Detailed Description
The `byteale` function implements the less-than-or-equal-to comparison operator for bytea data type in PostgreSQL. It performs lexicographic (dictionary-style) comparison between two binary string values, returning true if the first argument is less than or equal to the second. The function compares bytes sequentially using `memcmp()` up to the length of the shorter string. If the byte comparison shows the first string is lexicographically smaller, it returns true. If the compared portions are identical, it returns true if the first string's length is less than or equal to the second string's length.

The logic is similar to `bytealt` but includes the equality case in the final comparison (len1 <= len2 instead of len1 < len2).

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
  - Used as the less-than-or-equal-to operator function for bytea type in SQL operations
  - Referenced by the PostgreSQL type system for bytea ordering operations

## Notes and Other Information
- Implements lexicographic (dictionary-style) ordering for binary data
- Returns true if arg1 <= arg2 in lexicographic order
- Includes both less-than and equal-to cases in the comparison logic
- Part of the bytea comparison function family in varlena.c
- Memory-safe implementation with proper cleanup
- Located in src/backend/utils/adt/varlena.c:3878-3897

## Simplified Source

```c
Datum byteale(PG_FUNCTION_ARGS) {
    // Get the two bytea arguments
    bytea *arg1 = PG_GETARG_BYTEA_PP(0);
    bytea *arg2 = PG_GETARG_BYTEA_PP(1);

    // Get lengths excluding headers
    int len1 = VARSIZE_ANY_EXHDR(arg1);
    int len2 = VARSIZE_ANY_EXHDR(arg2);

    // Compare bytes up to the length of shorter string
    int cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

    // Clean up memory if needed
    PG_FREE_IF_COPY(arg1, 0);
    PG_FREE_IF_COPY(arg2, 1);

    // Return true if arg1 <= arg2 lexicographically
    // Either bytes differ (cmp < 0) or bytes same but arg1 not longer
    PG_RETURN_BOOL((cmp < 0) || ((cmp == 0) && (len1 <= len2)));
}
```