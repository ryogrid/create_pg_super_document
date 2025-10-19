# byteaoverlay_no_len

## Location
[src/backend/utils/adt/varlena.c:3106-3117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L3106-L3117)

## Overview
A PostgreSQL function that implements a two-argument variant of the OVERLAY() operation for bytea data types, where the replacement length defaults to the length of the replacement string.

## Definition

```c
Datum
byteaoverlay_no_len(PG_FUNCTION_ARGS)
```
## Detailed Description
The `byteaoverlay_no_len` function provides a simplified interface to the OVERLAY() operation for bytea data types when no explicit replacement length is specified. Unlike the four-argument `byteaoverlay` function, this variant takes only three arguments: the target bytea, the replacement bytea, and the starting position. The length of the substring to replace is automatically determined by the length of the replacement bytea using `VARSIZE_ANY_EXHDR(t2)`. This corresponds to the SQL OVERLAY() function when called with three arguments instead of four.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: `bytea *t1` - The target bytea string to be modified
  - Argument 1: `bytea *t2` - The replacement bytea string
  - Argument 2: `int sp` - The substring start position (1-based)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BYTEA_PP (for extracting bytea arguments)
  - PG_GETARG_INT32 (for extracting integer arguments)
  - VARSIZE_ANY_EXHDR (for getting the length of the replacement bytea)
  - [bytea_overlay](bytea_overlay.md) (core overlay implementation)
  - PG_RETURN_BYTEA_P (for returning bytea result)
- Called from:
  - SQL OVERLAY() function invocations on bytea data with three arguments

## Notes and Other Information
- This is a convenience wrapper for the common case where replacement length equals the replacement string length
- The replacement length `sl` is automatically calculated as the size of `t2` excluding headers
- Provides cleaner SQL syntax when full replacement of a substring is desired
- Still uses 1-based indexing for the start position as per SQL standard
- Located in src/backend/utils/adt/varlena.c:3106-3117

## Simplified Source

```c
// PostgreSQL function implementing 3-argument OVERLAY() for bytea data
Datum byteaoverlay_no_len(PG_FUNCTION_ARGS) {
    // Extract arguments: target bytea, replacement bytea, start position
    bytea *target = PG_GETARG_BYTEA_PP(0);
    bytea *replacement = PG_GETARG_BYTEA_PP(1);
    int start_pos = PG_GETARG_INT32(2);    // substring start position

    // Default replacement length to the size of replacement bytea
    int replacement_length = VARSIZE_ANY_EXHDR(replacement);

    // Delegate to core overlay function and return result
    return PG_RETURN_BYTEA_P(bytea_overlay(target, replacement, start_pos, replacement_length));
}
```