# byteaoverlay

## Location
[src/backend/utils/adt/varlena.c:3095-3105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L3095-L3105)

## Overview
A PostgreSQL function that implements the SQL standard OVERLAY() operation for bytea data types, replacing a specified substring of the first bytea with a second bytea.

## Definition


## Detailed Description
The  function provides a direct implementation of the SQL OVERLAY() function for bytea (binary string) data types. It follows the SQL standard definition which operates in terms of substring extraction and concatenation. The function takes four arguments: the target bytea, the replacement bytea, the starting position, and the length of the substring to replace. It serves as a wrapper around the core  function, handling PostgreSQL's function calling conventions and argument extraction.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0:  - The target bytea string to be modified
  - Argument 1:  - The replacement bytea string
  - Argument 2:  - The substring start position (1-based)
  - Argument 3:  - The substring length to replace

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BYTEA_PP (for extracting bytea arguments)
  - PG_GETARG_INT32 (for extracting integer arguments)
  - [bytea_overlay](bytea_overlay.md) (core overlay implementation)
  - PG_RETURN_BYTEA_P (for returning bytea result)
- Called from:
  - SQL OVERLAY() function invocations on bytea data

## Notes and Other Information
- This function is a thin wrapper that handles PostgreSQL's C function interface
- The actual overlay logic is implemented in the  helper function
- Follows the SQL standard specification for OVERLAY() operations
- Position arguments use 1-based indexing as per SQL standard
- Located in src/backend/utils/adt/varlena.c:3095-3105