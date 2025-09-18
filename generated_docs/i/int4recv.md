# int4recv

## Location
src/backend/utils/adt/int.c: 311 - 321

## Overview
Converts external binary format data to a 32-bit integer (int4) for PostgreSQL internal use.

## Definition
```c
Datum int4recv(PG_FUNCTION_ARGS)
```

## Detailed Description
The int4recv function is a PostgreSQL type receive function that converts data from external binary format into the internal representation of a 32-bit signed integer. This function is part of PostgreSQL's binary I/O system and is used when receiving integer data in binary format from clients or during binary data transfers. It reads a 4-byte integer from a StringInfo buffer using the PostgreSQL message protocol functions.

## Parameters / Member Variables
- No explicit parameters (uses PG_FUNCTION_ARGS macro)
- Input: StringInfo buffer containing binary data retrieved via PG_GETARG_POINTER(0)
- Output: Datum containing the converted 32-bit integer

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgint](../p/pq_getmsgint.md)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Part of PostgreSQL's binary protocol support for integer types
- Uses pq_getmsgint to safely extract a 4-byte integer from the message buffer
- The function follows PostgreSQL's fmgr (function manager) calling convention
- Located in src/backend/utils/adt/int.c:311-321
- Counterpart to int4send for binary data handling