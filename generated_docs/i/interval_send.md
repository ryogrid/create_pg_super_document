# interval_send

## Location
src/backend/utils/adt/timestamp.c: 1031 - 1055

## Overview
Converts PostgreSQL's internal Interval data type to its external binary format for efficient client-server communication using the binary protocol.

## Definition
```c
Datum interval_send(PG_FUNCTION_ARGS)
```

## Detailed Description
The `interval_send` function is the binary output conversion function for PostgreSQL's interval data type. It takes an internal Interval structure and serializes it into a binary format suitable for transmission over the client-server wire protocol. The function uses PostgreSQL's message buffer infrastructure to create an endian-safe binary representation of the interval's three components (time, day, month) that can be efficiently transmitted and later reconstructed by `interval_recv` on the receiving end.

## Parameters / Member Variables
- `interval` (PG_GETARG_INTERVAL_P(0)): The input Interval structure to be converted to binary format

## Dependencies
- Functions called/Symbols referenced:
  - [pq_begintypsend](../p/pq_begintypsend.md) (initialize binary message buffer)
  - [pq_sendint64](../p/pq_sendint64.md) (write 64-bit integer to binary buffer)
  - [pq_sendint32](../p/pq_sendint32.md) (write 32-bit integer to binary buffer)
  - [pq_endtypsend](../p/pq_endtypsend.md) (finalize binary message buffer)
  - PG_RETURN_BYTEA_P (macro for returning binary data result)
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's type system for binary protocol handling)

## Notes and Other Information
- Part of PostgreSQL's binary protocol infrastructure for efficient client-server communication
- Writes interval components in a specific order: time (microseconds as 64-bit), day (32-bit), month (32-bit)
- Uses PostgreSQL's message buffer functions (`pq_send*`) for endian-safe binary writing
- Creates a self-contained binary representation that includes proper length prefixes
- The binary format is more efficient than text formatting for high-volume data transfer
- Complements `interval_recv` for bidirectional binary conversion
- Returns a bytea (binary array) containing the serialized interval data