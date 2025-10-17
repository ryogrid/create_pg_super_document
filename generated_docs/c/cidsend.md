# cidsend

## Location
[src/backend/utils/adt/xid.c:359-369](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid.c#L359-L369)

## Overview
A PostgreSQL internal function that converts a CommandId (cid) value to binary format for efficient transmission over the wire protocol.

## Definition
```c
Datum cidsend(PG_FUNCTION_ARGS)
```

## Detailed Description
The `cidsend` function is part of PostgreSQL's binary protocol infrastructure for the CommandId type. It handles the conversion of CommandId values to binary format for efficient transmission over the client-server wire protocol. The function uses PostgreSQL's standard binary serialization utilities to package the CommandId as a binary message. This is the counterpart to the binary receive function and is essential for optimized client-server communication when binary format is preferred over text format.

## Parameters / Member Variables
- Input: CommandId value retrieved via `PG_GETARG_COMMANDID(0)` from function arguments
- Output: Returns binary data as bytea via `PG_RETURN_BYTEA_P`

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_COMMANDID (macro for extracting CommandId from function args)
  - CommandId (PostgreSQL internal type)
  - [StringInfoData](../S/StringInfoData.md) (PostgreSQL string buffer structure)
  - [pq_begintypsend](../p/pq_begintypsend.md) (PostgreSQL function to initialize binary send buffer)
  - [pq_sendint32](../p/pq_sendint32.md) (PostgreSQL function to add 32-bit integer to buffer)
  - [pq_endtypsend](../p/pq_endtypsend.md) (PostgreSQL function to finalize binary send buffer)
  - PG_RETURN_BYTEA_P (macro for returning binary data)

- Called from (representative examples):
  - No direct references found in the codebase (likely called through PostgreSQL's type system)

## Notes and Other Information
- Located in src/backend/utils/adt/xid.c:359-369
- Part of the CommandId type's binary input/output function suite
- Uses PostgreSQL's standard binary protocol utilities for consistent serialization
- Sends the CommandId as a 32-bit integer in the binary stream
- Follows PostgreSQL's standard pattern for type send functions using the PG_FUNCTION_ARGS interface
- The binary format is more efficient than text format for network transmission

## Simplified Source

```c
Datum cidsend(PG_FUNCTION_ARGS) {
    // Extract CommandId from function arguments
    CommandId command_id = PG_GETARG_COMMANDID(0);

    // Create binary output buffer
    StringInfoData buf;
    pq_begintypsend(&buf);

    // Send CommandId as 32-bit integer
    pq_sendint32(&buf, command_id);

    // Return serialized binary data
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}
```