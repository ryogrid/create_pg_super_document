# int4send

## Location
src/backend/utils/adt/int.c: 322 - 339

## Overview
Converts a 32-bit integer (int4) to binary format for external transmission in PostgreSQL.

## Definition
```c
Datum int4send(PG_FUNCTION_ARGS)
```

## Detailed Description
The int4send function is a PostgreSQL type send function that converts a 32-bit signed integer from its internal representation to external binary format. This function is part of PostgreSQL's binary I/O system and is used when sending integer data in binary format to clients or during binary data transfers. It creates a StringInfo buffer, writes the integer value in network byte order, and returns the resulting binary data as a bytea type.

## Parameters / Member Variables
- No explicit parameters (uses PG_FUNCTION_ARGS macro)
- Input: 32-bit signed integer retrieved via PG_GETARG_INT32(0)
- Output: Datum containing bytea with the binary representation of the integer

## Dependencies
- Functions called/Symbols referenced:
  - [pq_begintypsend](../p/pq_begintypsend.md)
  - [pq_sendint32](../p/pq_sendint32.md)
  - [pq_endtypsend](../p/pq_endtypsend.md)
  - PG_RETURN_BYTEA_P
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Part of PostgreSQL's binary protocol support for integer types
- Uses the standard PostgreSQL message buffer functions for consistent binary format
- The resulting binary data is in network byte order for portability
- The function follows PostgreSQL's fmgr (function manager) calling convention
- Located in src/backend/utils/adt/int.c:322-339
- Counterpart to int4recv for binary data handling