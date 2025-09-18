# path_send

## Location
src/backend/utils/adt/geo_ops.c: 1526 - 1552

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
  - pq_begintypsend (initializes binary output buffer)
  - pq_sendbyte (sends single byte - closed flag)
  - pq_sendint32 (sends 32-bit integer - point count)
  - pq_sendfloat8 (sends 64-bit float - coordinates)
  - pq_endtypsend (finalizes binary output buffer)
  - PG_RETURN_BYTEA_P (returns binary data as bytea)
- Called from (representative examples):
  - No direct references found (likely referenced through function pointer tables)

## Notes and Other Information
- Companion function to `path_recv` for binary I/O operations
- Output format: closed flag (1 byte) + point count (4 bytes) + point coordinates (16 bytes per point)
- Uses StringInfoData buffer for efficient binary data construction
- Part of PostgreSQL's type input/output function framework for the PATH geometric data type