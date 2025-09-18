# uuid_send

## Location
src/backend/utils/adt/uuid.c: 156 - 167

## Overview
PostgreSQL binary send function that serializes a UUID to binary wire format for client-server communication.

## Definition
```c
Datum uuid_send(PG_FUNCTION_ARGS)
```

## Detailed Description
The `uuid_send` function is a PostgreSQL binary send function that serializes a UUID to binary wire format for transmission between server and client. This function is used during client-server communication when UUIDs need to be transmitted in binary form for efficiency. 

The function creates a message buffer, writes the UUID's raw 16-byte binary data directly to the buffer, and returns the buffer as a bytea for network transmission. This provides efficient binary serialization without the overhead of text conversion.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro providing access to:
  - Input UUID (retrieved via `PG_GETARG_UUID_P(0)`)
  - Function call context information

## Dependencies
- Functions called/Symbols referenced:
  - `[pq_begintypsend](../p/pq_begintypsend.md)` (initializes message buffer for type sending)
  - `pq_sendbytes` (writes binary data to message buffer)
  - `[pq_endtypsend](../p/pq_endtypsend.md)` (finalizes message buffer and returns bytea)
  - `PG_GETARG_UUID_P` (argument retrieval macro for UUID)
  - `PG_RETURN_BYTEA_P` (return value macro for bytea)
- Constants used:
  - `UUID_LEN` (UUID length in bytes, typically 16)
- Types used:
  - `[pg_uuid_t](../p/pg_uuid_t.md)` (internal UUID structure)
  - `[StringInfoData](../S/StringInfoData.md)` (PostgreSQL message buffer type)
- Called from:
  - PostgreSQL type system (automatically during binary protocol communication)

## Notes and Other Information
- This function handles binary wire format output, complementing `uuid_recv` for input
- No conversion or validation is needed - raw binary data is transmitted directly
- Used specifically for PostgreSQL's binary protocol communication between server and client
- More efficient than text-based transmission for large numbers of UUIDs
- Part of PostgreSQL's binary I/O system for optimized network communication
- The function is registered in the type system for UUID binary output operations
- Returns a bytea containing exactly `UUID_LEN` bytes of UUID data