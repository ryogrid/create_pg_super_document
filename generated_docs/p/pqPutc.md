# pqPutc

## Location
[src/interfaces/libpq/fe-misc.c:92-108](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-misc.c#L92-L108)

## Overview
Writes a single character to the current outgoing message being constructed for the PostgreSQL backend.

## Definition

```c
int
pqPutc(char c, PGconn *conn)
```
## Detailed Description
pqPutc is a convenience function that writes a single character to the outgoing message buffer. It is a thin wrapper around pqPutMsgBytes that simplifies the process of adding a single character to a message being constructed for transmission to the PostgreSQL backend.

The function takes a character and adds it to the current message in the connection's output buffer. This is commonly used when building protocol messages that require specific character values, such as message type indicators, terminators, or single-character flags.

pqPutc is part of the message construction infrastructure in libpq and is used internally when building various types of protocol messages before they are sent to the backend.

## Parameters / Member Variables
- `c`: The character to write to the message buffer
- `*conn`: Pointer to the PGconn structure representing the database connection
## Dependencies
- Functions called/Symbols referenced:
  - [pqPutMsgBytes](pqPutMsgBytes.md) (adds bytes to the outgoing message buffer)
- Called from (representative examples):
  - [PQsendQueryGuts](../P/PQsendQueryGuts.md) (query message construction)
  - [PQsendTypedCommand](../P/PQsendTypedCommand.md) (typed command message construction)

## Notes and Other Information
- Returns 0 on success, EOF on error
- This is an internal libpq function, not part of the public API
- Part of the message construction infrastructure for the PostgreSQL protocol
- The character is buffered and not immediately sent to the network
- Error conditions are typically related to buffer space allocation failures
- Thread-safety depends on the connection's locking mechanisms