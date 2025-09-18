# pqGetNegotiateProtocolVersion3

## Location
[src/interfaces/libpq/fe-protocol3.c:1412-1468](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-protocol3.c#L1412-L1468)

## Overview
Processes a NegotiateProtocolVersion message from the PostgreSQL server during connection negotiation to handle protocol version mismatches and unsupported extensions.

## Definition


## Detailed Description
This function handles the NegotiateProtocolVersion message ('v') that the server sends when there's a protocol version mismatch or when the client requests protocol extensions that the server doesn't support. The message contains the server's supported protocol version and a list of unrecognized protocol extensions.

The function reads the server's protocol version, then reads a list of unsupported extension names. It constructs appropriate error messages based on whether the issue is a protocol version mismatch, unsupported extensions, or both. The function ensures proper error reporting with internationalized messages for both single and plural extension cases.

## Parameters / Member Variables
- : PostgreSQL connection object containing connection state and buffers for reading the message data

## Dependencies
- Functions called/Symbols referenced:
  - [pqGetInt](pqGetInt.md)
  - initPQExpBuffer
  - [pqGets](pqGets.md)
  - termPQExpBuffer
  - appendPQExpBufferChar
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
  - PG_PROTOCOL_MAJOR
  - PG_PROTOCOL_MINOR
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [libpq_ngettext](../l/libpq_ngettext.md)
- Called from (representative examples):
  - Connection negotiation routines (indirectly referenced from libpq-int.h)

## Notes and Other Information
- Returns 0 on successful message consumption, EOF if insufficient data available
- Message format: protocol version (4 bytes), number of extensions (4 bytes), followed by extension name strings
- Handles both protocol version downgrades and unsupported extension reporting
- Uses libpq_ngettext for proper singular/plural error message formatting
- Validates that the server sent the message for a valid reason (version mismatch or extensions)
- Error messages include specific version numbers in major.minor format
- Temporary buffer is used to accumulate extension names with space separation
- Function entry assumes 'v' message type and length have already been consumed