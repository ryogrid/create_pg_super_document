# be_gssapi_read

## Location
src/backend/libpq/be-secure-gssapi.c: 269 - 429

## Overview
Reads data from a GSSAPI-encrypted connection, handling decryption of incoming packets and buffering of decrypted data for the caller.

## Definition


## Detailed Description
The  function reads up to  bytes of data from a GSSAPI-encrypted connection into the provided buffer. It operates by:

1. Reading encrypted packets from the network into 
2. Decrypting complete packets using GSSAPI  into 
3. Copying decrypted data from the result buffer to the caller's buffer
4. Managing partial reads and buffering across multiple function calls

The function uses a two-stage buffering approach: it first collects complete encrypted packets (including a 4-byte length prefix), then decrypts them entirely before serving data to callers. This design handles the fact that GSSAPI encryption works on complete packets rather than streaming data.

For non-blocking sockets, the function may return with  if insufficient data is available to complete a packet read or decrypt operation.

## Parameters / Member Variables
- : Pointer to Port structure containing the connection state and GSSAPI context
- : Buffer where decrypted data will be stored
- : Maximum number of bytes to read into the buffer

## Dependencies
- Functions called/Symbols referenced:
  - : Reads raw encrypted data from the underlying socket
  - : GSSAPI function to decrypt and verify integrity of received data
  - : Releases GSSAPI-allocated buffer memory
  - : PostgreSQL function to report GSSAPI errors
  - : Network-to-host byte order conversion for 32-bit integers
- Global buffers used:
  - : Buffer for incoming encrypted packets
  - : Buffer for decrypted data
  - , , : Buffer state variables
- Called from:
  - : Main secure read dispatcher function

## Notes and Other Information
- Requires that GSSAPI transport negotiation has already been completed
- Returns the number of bytes actually read, or -1 on error with errno set
- Uses confidentiality checking to ensure incoming packets were properly encrypted
- Enforces maximum packet size limits () to prevent memory exhaustion attacks
- The function is designed to avoid infinite recursion issues by treating fatal errors consistently
- May return fewer bytes than requested even when more data is available, allowing caller to process data incrementally