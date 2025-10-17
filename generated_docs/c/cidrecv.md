# cidrecv

## Location
[src/backend/utils/adt/xid.c:348-358](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid.c#L348-L358)

## Overview
A PostgreSQL internal function that converts external binary format data to a CommandId (cid) value, used for binary protocol communication.

## Definition
```c
Datum cidrecv(PG_FUNCTION_ARGS)
```

## Detailed Description
The `cidrecv` function is part of PostgreSQL's binary protocol infrastructure for the CommandId type. It handles the reception and conversion of CommandId values sent in binary format over the wire protocol. The function reads binary data from a StringInfo buffer using PostgreSQL's message parsing utilities and converts it back to the internal CommandId representation. This is the counterpart to the binary send function and is essential for client-server communication when binary format is used.

## Parameters / Member Variables
- Input: StringInfo buffer retrieved via `PG_GETARG_POINTER(0)` containing binary data
- Output: Returns a CommandId value via `PG_RETURN_COMMANDID`

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER (macro for extracting pointer from function args)
  - StringInfo (PostgreSQL string buffer type)
  - [pq_getmsgint](../p/pq_getmsgint.md) (PostgreSQL function to extract integer from message buffer)
  - CommandId (PostgreSQL internal type)
  - PG_RETURN_COMMANDID (macro for returning CommandId)

- Called from (representative examples):
  - No direct references found in the codebase (likely called through PostgreSQL's type system)

## Notes and Other Information
- Located in src/backend/utils/adt/xid.c:348-358
- Part of the CommandId type's binary input/output function suite
- Works with PostgreSQL's binary protocol for efficient data transfer
- Uses pq_getmsgint to extract the CommandId from the binary message buffer
- Follows PostgreSQL's standard pattern for type receive functions using the PG_FUNCTION_ARGS interface

## Simplified Source

```c
Datum cidrecv(PG_FUNCTION_ARGS) {
    StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);

    // Read CommandId from binary buffer and return it
    PG_RETURN_COMMANDID((CommandId) pq_getmsgint(buf, sizeof(CommandId)));
}
```