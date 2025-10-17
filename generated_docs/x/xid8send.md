# xid8send

## Location
[src/backend/utils/adt/xid.c:212-222](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid.c#L212-L222)

## Overview
Serializes a FullTransactionId (XID8) value to binary format for transmission over PostgreSQL's binary protocol.

## Definition

```c
Datum
xid8send(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is PostgreSQL's binary output function for the XID8 data type. It serializes a FullTransactionId to binary format suitable for transmission over PostgreSQL's binary protocol or storage in binary format. This function is the counterpart to  and is automatically invoked by PostgreSQL's type system when binary serialization is required.

The function creates a StringInfo buffer, extracts the 64-bit value from the FullTransactionId using , and writes it as a 64-bit integer to the buffer using PostgreSQL's message building functions. The resulting binary data is returned as a bytea value.

## Parameters / Member Variables
- Input parameter (via PG_FUNCTION_ARGS): A FullTransactionId (XID8) value to be serialized

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts FullTransactionId from function arguments
  -  - Initializes binary message buffer for type serialization
  -  - Extracts uint64 from FullTransactionId
  -  - Writes 64-bit integer to message buffer
  -  - Finalizes binary message buffer
  -  - Returns binary data as PostgreSQL bytea Datum
- Types referenced:
  -  - 64-bit transaction identifier type
  -  - PostgreSQL message buffer structure
- Called from:
  - No direct callers found (invoked automatically by PostgreSQL's type system during binary serialization)

## Notes and Other Information
- Essential for PostgreSQL's binary protocol support, enabling efficient transmission of XID8 values
- Works in conjunction with  to provide complete binary serialization/deserialization
- Uses PostgreSQL's standard message building infrastructure for consistent binary format
- The binary format is platform-independent, ensuring compatibility across different systems
- Returned bytea can be stored, transmitted, or processed by binary-aware PostgreSQL functions
- Located in src/backend/utils/adt/xid.c alongside other transaction ID utility functions

## Simplified Source

```c
Datum xid8send(PG_FUNCTION_ARGS) {
    FullTransactionId full_xid = PG_GETARG_FULLTRANSACTIONID(0);
    StringInfoData buf;

    // Initialize binary output buffer
    pq_begintypsend(&buf);

    // Convert FullTransactionId to uint64 and write as 64-bit integer
    pq_sendint64(&buf, (uint64) U64FromFullTransactionId(full_xid));

    // Return the binary data
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}
```