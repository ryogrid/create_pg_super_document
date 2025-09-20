# uuid_recv

## Location
[src/backend/utils/adt/uuid.c:145-155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/uuid.c#L145-L155)

## Overview
PostgreSQL binary receive function that reads a UUID from binary wire format during client-server communication.

## Definition
```c
Datum uuid_recv(PG_FUNCTION_ARGS)
```

## Detailed Description
The `uuid_recv` function is a PostgreSQL binary receive function that deserializes a UUID from binary wire format. This function is used during client-server communication when UUIDs are transmitted in binary form (as opposed to text format). It reads exactly `UUID_LEN` bytes from the input buffer and copies them directly into a newly allocated UUID structure.

The function operates on the assumption that UUIDs are transmitted in their raw 16-byte binary representation, which is then directly copied into the internal `pg_uuid_t` structure without any conversion or validation.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro providing access to:

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (memory allocation for UUID structure)
  - [pq_getmsgbytes](../p/pq_getmsgbytes.md) (reads bytes from message buffer) 
  - `memcpy` (copies binary data)
  - `PG_GETARG_POINTER` (argument retrieval macro)
  - `PG_RETURN_POINTER` (return value macro)
- Constants used:
  - `UUID_LEN` (UUID length in bytes, typically 16)
- Types used:
  - [pg_uuid_t](../p/pg_uuid_t.md) (internal UUID structure)
  - `StringInfo` (PostgreSQL message buffer type)
- Called from:
  - PostgreSQL type system (automatically during binary protocol communication)

## Notes and Other Information
- This function handles binary wire format, not text format
- No validation is performed on the received data - it assumes the binary data is valid
- Used specifically for PostgreSQL's binary protocol communication between client and server
- Allocates exactly `UUID_LEN` bytes for the UUID structure
- Part of PostgreSQL's binary I/O system for efficient network communication
- The function is registered in the type system for UUID binary input operations