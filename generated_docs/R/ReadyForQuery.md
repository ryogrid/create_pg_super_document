# ReadyForQuery

## Location
[src/backend/tcop/dest.c:256-286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/dest.c#L256-L286)

## Overview
ReadyForQuery signals to clients that the server is ready to accept a new query, including transaction state information in protocol version 3.0 and above.

## Definition
```c
void ReadyForQuery(CommandDest dest)
```

## Detailed Description
ReadyForQuery is a critical function in PostgreSQL's client-server communication protocol that indicates the server has finished processing the current query and is ready to accept new commands. For remote destinations, it sends a ReadyForQuery message containing the current transaction block status code, allowing clients to understand both query completion and transaction state.

The function also performs an important optimization by flushing the output buffer, which reduces the number of separate network packets sent and improves communication efficiency. This is particularly important in high-frequency query scenarios where minimizing network overhead is crucial for performance.

In PostgreSQL protocol version 3.0 and later, the ReadyForQuery message includes a transaction state indicator (via TransactionBlockStatusCode) that tells the client whether it's in a transaction block, idle, or in a failed transaction state.

## Parameters / Member Variables
- `dest`: CommandDest enumeration value specifying the command output destination

## Dependencies
- Functions called/Symbols referenced:
  - pq_beginmessage (starts building a protocol message)
  - [pq_sendbyte](../p/pq_sendbyte.md) (adds a byte to the message)
  - [pq_endmessage](../p/pq_endmessage.md) (finalizes the protocol message)
  - pq_flush (flushes output buffer to reduce network packets)
  - [TransactionBlockStatusCode](../T/TransactionBlockStatusCode.md) (gets current transaction state indicator)
  - PqMsg_ReadyForQuery (protocol message type constant)
  - CommandDest enum values (DestRemote, DestRemoteExecute, etc.)
- Called from (representative examples):
  - [PostgresMain](../P/PostgresMain.md) (in postgres.c)

## Notes and Other Information
- Located in src/backend/tcop/dest.c:256-286
- Essential for PostgreSQL frontend/backend protocol compliance
- Includes transaction state information since protocol version 3.0
- Performs buffer flushing optimization to reduce network overhead
- Only processes remote destinations - local destinations require no special handling
- Critical for client applications to know when they can send the next command
- Part of the fundamental query processing cycle in PostgreSQL
- Works in conjunction with TransactionBlockStatusCode to provide complete state information