# socket_putmessage

## Location
src/backend/libpq/pqcomm.c: 1488 - 1520

## Overview
A static function that sends a complete PostgreSQL protocol message to the client, including the message type, length header, and message body data.

## Definition


## Detailed Description
This function constructs and sends a complete PostgreSQL protocol message following the standard format: message type (1 byte) + message length (4 bytes, including the length field itself) + message body. The function handles the protocol-level details of message formatting and uses internal buffering mechanisms to queue the data for transmission.

The function includes safety mechanisms to prevent message interleaving by using the PqCommBusy flag, which suppresses messages while communication operations are in progress. This is particularly important to prevent issues when signals (like SIGQUIT) might trigger message sending during ongoing communication operations.

## Parameters / Member Variables
- : Single character identifying the PostgreSQL protocol message type (must not be 0)
- : Pointer to the message body data to be sent
- : Length of the message body data in bytes

Returns:
- : Success - message was successfully queued for transmission
- : Error occurred during the message construction or buffering

## Dependencies
- Functions called/Symbols referenced:
  -  (lines 1497, 1501, 1504) - places raw bytes into the send buffer
  -  (line 1500) - converts 32-bit integer to network byte order
- Global variables accessed:
  -  - flag to prevent reentrant calls and message interleaving

## Notes and Other Information
- This is a static function, only accessible within the pqcomm.c file
- The message length field includes the 4 bytes of the length field itself (len + 4)
- Uses network byte order (big-endian) for the length field via pg_hton32()
- The function suppresses messages when PqCommBusy is true to avoid message corruption
- Implements proper error handling with a goto fail pattern for cleanup
- The Assert(msgtype != 0) ensures that null message types are caught during development
- Message format follows PostgreSQL's frontend/backend protocol specification
- Messages sent by this function are suppressed in COPY OUT mode as indicated in the comment
- Part of the message-level I/O routines in PostgreSQL's communication layer