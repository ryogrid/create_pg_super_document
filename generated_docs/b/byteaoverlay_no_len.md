# byteaoverlay_no_len

## Location
src/backend/utils/adt/varlena.c: 3106 - 3117

## Overview
A PostgreSQL function that implements a two-argument variant of the OVERLAY() operation for bytea data types, where the replacement length defaults to the length of the replacement string.

## Definition


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
  - bytea_overlay (core overlay implementation)
  - PG_RETURN_BYTEA_P (for returning bytea result)
- Called from:
  - SQL OVERLAY() function invocations on bytea data with three arguments

## Notes and Other Information
- This is a convenience wrapper for the common case where replacement length equals the replacement string length
- The replacement length `sl` is automatically calculated as the size of `t2` excluding headers
- Provides cleaner SQL syntax when full replacement of a substring is desired
- Still uses 1-based indexing for the start position as per SQL standard
- Located in src/backend/utils/adt/varlena.c:3106-3117