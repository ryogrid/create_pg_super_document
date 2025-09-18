# byteapos

## Location
src/backend/utils/adt/varlena.c: 3165 - 3208

## Overview
A PostgreSQL function that finds and returns the position of a specified substring within a bytea value, implementing the SQL POSITION() function for binary string data types.

## Definition


## Detailed Description
The `byteapos` function implements the SQL standard POSITION() function for bytea data types. It searches for the first occurrence of a substring (pattern) within a target bytea string and returns its 1-based position. The function performs a byte-by-byte comparison using `memcmp` for exact binary matching. If the pattern is found, it returns the position where the match starts (1-based indexing). If no match is found, it returns 0. The function is cloned from the text version (`textpos`) and modified to work with binary data. Special handling is provided for empty patterns, which always return position 1 according to SQL standards.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: `bytea *t1` - The target bytea string to search within
  - Argument 1: `bytea *t2` - The pattern bytea string to search for

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BYTEA_PP (for extracting bytea arguments)
  - VARSIZE_ANY_EXHDR (for getting the data size excluding headers)
  - VARDATA_ANY (for getting pointer to the actual data)
  - memcmp (for binary comparison of byte sequences)
  - PG_RETURN_INT32 (for returning 32-bit integer result)
- Called from:
  - SQL POSITION() function invocations on bytea data

## Notes and Other Information
- Uses 1-based indexing for positions as per SQL standard
- Returns 0 when no match is found
- Returns 1 for empty pattern searches (SQL standard behavior)
- Performs exact binary matching using `memcmp`
- Cloned and adapted from the text-based `textpos` function
- Uses efficient linear search algorithm with early termination
- Handles binary data that may contain null bytes
- Located in src/backend/utils/adt/varlena.c:3165-3208