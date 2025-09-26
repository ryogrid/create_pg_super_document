# pq_getmessage

## Location
[src/backend/libpq/pqcomm.c:1202-1275](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L1202-L1275)

## Overview
Reads a complete message with length word from a PostgreSQL client connection, placing the message body in an expandable StringInfo buffer.

## Definition

```c
int
pq_getmessage(StringInfo s, int maxlen)
```
## Detailed Description
The  function is a core component of PostgreSQL's client-server communication protocol. It reads a complete message from the client connection, starting with a 4-byte length word followed by the message body. The function handles protocol validation, memory management, and error recovery to maintain communication synchronization.

The function first reads the 4-byte message length in network byte order, validates it against the specified maximum length, then allocates space and reads the message body. It includes robust error handling to discard oversized messages while maintaining protocol sync, and uses PostgreSQL's exception handling mechanism (PG_TRY/PG_CATCH) for memory allocation failures.

## Parameters / Member Variables
- : StringInfo buffer to store the received message body (length word is stripped)
- : Maximum allowed message length in bytes; connections are terminated if exceeded

## Dependencies
- Functions called/Symbols referenced:
  - [resetStringInfo](../r/resetStringInfo.md)
  - [pq_getbytes](pq_getbytes.md)
  - pg_ntoh32
  - [enlargeStringInfo](../e/enlargeStringInfo.md)
  - [pq_discardbytes](pq_discardbytes.md)
  - COMMERROR (error reporting)
  - PG_TRY/PG_CATCH/PG_RE_THROW (exception handling)
- Called from (representative examples):
  - [SocketBackend](../S/SocketBackend.md) (main message processing loop)
  - [CopyGetData](../C/CopyGetData.md) (COPY command data handling)
  - [CheckSASLAuth](../C/CheckSASLAuth.md) (SASL authentication)
  - [recv_password_packet](../r/recv_password_packet.md) (password authentication)
  - [ProcessRepliesIfAny](../P/ProcessRepliesIfAny.md) (replication feedback)

## Notes and Other Information
- Requires PqCommReadingMsg flag to be true before calling
- Automatically resets StringInfo cursor to zero for message scanning convenience
- Uses network byte order conversion for cross-platform compatibility  
- Implements graceful error recovery by discarding oversized messages to maintain protocol sync
- Returns 0 on success, EOF on communication errors
- Sets PqCommReadingMsg to false when message reading is complete