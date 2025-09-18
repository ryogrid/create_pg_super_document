# pqGets

## Location
[src/interfaces/libpq/fe-misc.c:136-141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-misc.c#L136-L141)

## Overview
Reads a null-terminated string from the connection's input buffer, replacing any existing content in the provided buffer.

## Definition


## Detailed Description
pqGets is a wrapper function around pqGets_internal that provides a convenient interface for reading null-terminated strings from the PostgreSQL connection's input buffer. It always resets the target buffer before reading the string, ensuring that any previous content is cleared.

This function is commonly used throughout libpq's protocol parsing code to extract string values from various message types received from the PostgreSQL backend. It's particularly useful for reading field names, parameter names, error messages, and other textual data that are transmitted as null-terminated strings in the protocol.

The function operates on buffered data that has already been received from the network, making it efficient for parsing multiple string values from a single received message.

## Parameters / Member Variables
- : PQExpBuffer where the extracted string will be stored (content will be reset)
- : Pointer to the PGconn structure representing the database connection

## Dependencies
- Functions called/Symbols referenced:
  - [pqGets_internal](pqGets_internal.md) (with resetbuffer=true to clear the buffer first)
- Called from (representative examples):
  - [pg_SASL_init](pg_SASL_init.md) (SASL authentication string processing)
  - [pqParseInput3](pqParseInput3.md) (protocol 3 message parsing)
  - [getRowDescriptions](../g/getRowDescriptions.md) (result set field name parsing)
  - [pqGetErrorNotice3](pqGetErrorNotice3.md) (error/notice message string parsing)
  - [pqGetNegotiateProtocolVersion3](pqGetNegotiateProtocolVersion3.md) (protocol negotiation)
  - [getParameterStatus](../g/getParameterStatus.md) (server parameter name/value parsing)
  - [getNotify](../g/getNotify.md) (notification channel/payload parsing)

## Notes and Other Information
- Returns 0 on success, EOF when a complete null-terminated string is not available
- This is an internal libpq function, not part of the public API
- Always clears the buffer before reading, unlike pqGets_append which preserves existing content
- Part of the core protocol message parsing infrastructure
- Does not perform network I/O; operates only on already buffered data
- Thread-safety depends on the connection's locking mechanisms
- Essential for parsing PostgreSQL wire protocol messages containing string data