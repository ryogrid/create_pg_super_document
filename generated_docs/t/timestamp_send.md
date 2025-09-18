# timestamp_send

## Location
src/backend/utils/adt/timestamp.c: 291 - 301

## Overview
Converts PostgreSQL timestamp data type to external binary format for serialization to network or storage.

## Definition
```c
Datum timestamp_send(PG_FUNCTION_ARGS)
```

## Detailed Description
The `timestamp_send` function is the counterpart to `timestamp_recv` in PostgreSQL's binary I/O interface for the timestamp data type. It serializes a PostgreSQL internal timestamp value into binary format that can be transmitted over the network or stored. The function creates a StringInfo buffer, writes the timestamp as a 64-bit integer using PostgreSQL's binary protocol functions, and returns the resulting bytea.

The implementation is straightforward: it extracts the timestamp argument, initializes a binary output buffer, writes the timestamp as a 64-bit integer, and returns the completed binary representation.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]` (Timestamp): The timestamp value to be converted to binary format

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TIMESTAMP
  - [pq_begintypsend](../p/pq_begintypsend.md)
  - [pq_sendint64](../p/pq_sendint64.md)
  - [pq_endtypsend](../p/pq_endtypsend.md)
  - PG_RETURN_BYTEA_P
- Called from (representative examples):
  - No direct references found in the current analysis

## Notes and Other Information
- This function is part of PostgreSQL's type system binary I/O interface
- Works in conjunction with `timestamp_recv` to provide complete binary I/O support
- The binary format is a simple 64-bit integer representation of the timestamp
- Used by PostgreSQL's binary protocol for efficient data transmission
- Located in src/backend/utils/adt/timestamp.c:291-301