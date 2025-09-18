# pqPacketSend

## Location
src/interfaces/libpq/fe-connect.c: 4986 - 5009

## Overview
A convenience function in libpq that sends a complete message packet to the PostgreSQL server, handling message framing, type specification, and ensuring delivery.

## Definition
```c
int pqPacketSend(PGconn *conn, char pack_type, const void *buf, size_t buf_len)
```

## Detailed Description
pqPacketSend is an internal libpq function that provides a high-level interface for sending complete protocol messages to the PostgreSQL server. The function handles the entire message transmission process, including message type framing, length encoding, content transmission, and flushing to ensure the server receives the data. It encapsulates the low-level message construction and transmission details, making it easier for other libpq functions to send properly formatted protocol messages.

The function follows PostgreSQL's wire protocol by starting each message with a type byte (except for startup packets), followed by a length field, then the message content. After constructing and sending the complete message, it flushes the connection to ensure immediate delivery to the server.

## Parameters / Member Variables
- `conn`: A pointer to the PGconn structure representing the database connection
- `pack_type`: The single-byte message type code that identifies the message type in PostgreSQL's protocol. Pass zero for startup packets which have no message type code
- `buf`: A pointer to the buffer containing the message content to be sent
- `buf_len`: The length of the message content in bytes, not including the message type and length fields which are added automatically

## Dependencies
- Functions called/Symbols referenced:
  - pqPutMsgStart
  - pqPutnchar
  - pqPutMsgEnd
  - pqFlush
  - STATUS_ERROR
  - STATUS_OK
- Called from (representative examples):
  - pg_GSS_continue
  - pg_SSPI_continue
  - pg_SASL_continue
  - pg_password_sendauth
  - Various connection-related functions

## Notes and Other Information
- Returns STATUS_OK on successful transmission, STATUS_ERROR on failure
- May block during execution as it waits for the message to be sent and flushed
- Automatically handles PostgreSQL wire protocol message framing
- Used primarily for authentication and connection establishment messages
- The function ensures atomicity by either sending the complete message or failing entirely
- Critical for maintaining protocol compliance during client-server communication
- Internal function not exposed in the public libpq API