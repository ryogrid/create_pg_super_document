# pg_logical_emit_message_text

## Location
[src/backend/replication/logical/logicalfuncs.c:382-386](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logicalfuncs.c#L382-L386)

## Overview
Writes a logical decoding message with textual data into the Write-Ahead Log (WAL), serving as a SQL function wrapper that delegates to the binary message variant for compatibility.

## Definition

```c
Datum
pg_logical_emit_message_text(PG_FUNCTION_ARGS)
```
## Detailed Description
 is a PostgreSQL SQL function implementation that provides a text-based interface for writing logical decoding messages into the WAL. The function is essentially a thin wrapper around , leveraging the compatibility between PostgreSQL's  and  data types.

This function is registered in the system catalog as  (OID 3577) with the signature , making it the textual variant of the logical message emission functionality. It allows applications to embed custom textual messages in the logical replication stream that can be consumed by logical decoding plugins.

The function follows PostgreSQL's standard function calling convention using  and returns a  representing the LSN (Log Sequence Number) where the message was written.

## Parameters / Member Variables
The function accepts parameters through the standard PostgreSQL function interface ():
- Parameter 0 (bool):  - Whether the message should be part of a transaction
- Parameter 1 (text):  - A prefix string to identify the message type or source
- Parameter 2 (text):  - The actual textual message content to be logged
- Parameter 3 (bool):  - Whether to immediately flush the WAL record to disk

## Dependencies
- Functions called/Symbols referenced:
  -  - The core implementation that handles the actual WAL writing
- Called from (representative examples):
  - SQL queries using 
  - Applications performing logical replication with custom messages

## Notes and Other Information
- The function leverages PostgreSQL's internal compatibility between  and  types, allowing the text variant to directly delegate to the binary implementation
- Located in 
- Part of PostgreSQL's logical replication infrastructure, introduced for allowing custom application messages in logical decoding streams
- The function is registered as volatile () since it modifies the WAL and cannot be optimized away
- Returns the LSN where the message was written, which can be used for tracking or synchronization purposes
- This is one of two overloaded variants of  - the other being the binary () version

## Simplified Source

```c
Datum pg_logical_emit_message_text(PG_FUNCTION_ARGS) {
    // Text and bytea types are compatible in PostgreSQL
    // Simply delegate to the binary version
    return pg_logical_emit_message_bytea(fcinfo);
}
```