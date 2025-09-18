# pq_putmessage_v2

## Location
src/backend/libpq/pqcomm.c: 1558 - 1589

## Overview
A function that sends messages using the deprecated PostgreSQL protocol version 2 format, maintained primarily for sending "unsupported protocol version" error messages to legacy clients.

## Definition


## Detailed Description
This function implements message sending for PostgreSQL's protocol version 2, which is no longer supported in current PostgreSQL versions. Unlike the modern protocol version 3 format used by socket_putmessage(), this function does not include a message length field in the protocol header - it sends only the message type followed directly by the message body.

The function is primarily retained as a compatibility mechanism to allow PostgreSQL to gracefully handle connection attempts from very old clients by sending them an "unsupported protocol version" error message in the format they can understand, before closing the connection.

Like other message functions, it includes the PqCommBusy mechanism to prevent message interleaving and maintains the same error handling patterns.

## Parameters / Member Variables
- : Single character identifying the PostgreSQL protocol message type (must not be 0)
- : Pointer to the message body data to be sent
- : Length of the message body data in bytes

Returns:
- : Success - message was successfully queued for transmission
- : Error occurred during the message construction or buffering

## Dependencies
- Functions called/Symbols referenced:
  -  (lines 1565, 1568) - places raw bytes into the send buffer
- Global variables accessed:
  -  - flag to prevent reentrant calls and message interleaving
- Referenced by:
  -  (src/backend/utils/error/elog.c:3638) - likely for error reporting
  -  (src/include/libpq/libpq.h:83) - for event handling scenarios

## Notes and Other Information
- Unlike socket_putmessage(), this is a non-static function accessible from other compilation units
- Protocol version 2 format: msgtype (1 byte) + message body (no length field)
- This contrasts with protocol version 3 format: msgtype (1 byte) + length (4 bytes) + message body
- The absence of a length field in v2 messages makes them less robust for parsing
- The function is kept purely for backward compatibility with very old PostgreSQL clients
- Uses the same PqCommBusy locking mechanism as modern message functions
- The Assert(msgtype != 0) ensures that null message types are caught during development
- Implements the same goto fail error handling pattern as other message functions
- This function serves as a bridge between modern PostgreSQL and legacy protocol requirements