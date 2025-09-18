# interval_recv

## Location
src/backend/utils/adt/timestamp.c: 1006 - 1030

## Overview
Converts PostgreSQL's external binary format to the internal Interval data type, used in binary protocol communication between client and server.

## Definition
```c
Datum interval_recv(PG_FUNCTION_ARGS)
```

## Detailed Description
The `interval_recv` function is the binary input conversion function for PostgreSQL's interval data type. It reads interval data from a binary message buffer (typically from client-server communication using the binary protocol) and constructs an internal Interval structure. The function reads the three components of an interval (time in microseconds, days, and months) from the binary stream in the expected wire format, then applies any type modifier constraints to ensure the result conforms to the declared interval type.

## Parameters / Member Variables
- `buf` (PG_GETARG_POINTER(0)): StringInfo buffer containing the binary representation of the interval
- `typelem` (unused): Type element OID (not currently used)
- `typmod` (PG_GETARG_INT32(2)): Type modifier specifying interval precision and range restrictions

## Dependencies
- Functions called/Symbols referenced:
  - palloc (allocate memory for Interval structure)
  - pq_getmsgint64 (read 64-bit integer from binary buffer)
  - pq_getmsgint (read integer from binary buffer)
  - AdjustIntervalForTypmod (apply type modifier constraints)
  - PG_RETURN_INTERVAL_P (macro for returning Interval pointer result)
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's type system for binary protocol handling)

## Notes and Other Information
- Part of PostgreSQL's binary protocol infrastructure for efficient client-server communication
- Reads interval components in a specific order: time (microseconds), day, month
- Uses PostgreSQL's message buffer functions (`pq_getmsg*`) for endian-safe binary reading
- Applies type modifier constraints after reading the binary data to ensure type compliance
- The binary format is more efficient than text parsing for high-volume data transfer
- Complements `interval_send` for bidirectional binary conversion