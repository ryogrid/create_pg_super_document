# path_send

## Location
[src/backend/utils/adt/geo_ops.c:1526-1552](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L1526-L1552)

## Overview
Binary output function that converts PostgreSQL's internal PATH data type to external binary format for transmission or storage.

## Definition


## Detailed Description
The `path_send` function serializes a PATH structure into PostgreSQL's binary format. It writes the closed flag as a single byte (1 for closed, 0 for open), followed by the number of points as a 32-bit integer, and then the x and y coordinates of each point as 64-bit floating point values. This is the inverse operation of `path_recv`.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `path`: PATH pointer to the internal path structure to be serialized

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_PATH_P (extracts PATH from function arguments)
  - [pq_begintypsend](pq_begintypsend.md) (initializes binary output buffer)
  - [pq_sendbyte](pq_sendbyte.md) (sends single byte - closed flag)
  - [pq_sendint32](pq_sendint32.md) (sends 32-bit integer - point count)
  - [pq_sendfloat8](pq_sendfloat8.md) (sends 64-bit float - coordinates)
  - [pq_endtypsend](pq_endtypsend.md) (finalizes binary output buffer)
  - PG_RETURN_BYTEA_P (returns binary data as bytea)
- Called from (representative examples):
  - No direct references found (likely referenced through function pointer tables)

## Notes and Other Information
- Companion function to `path_recv` for binary I/O operations
- Output format: closed flag (1 byte) + point count (4 bytes) + point coordinates (16 bytes per point)
- Uses StringInfoData buffer for efficient binary data construction
- Part of PostgreSQL's type input/output function framework for the PATH geometric data type