# read_or_wait

## Location
src/backend/libpq/be-secure-gssapi.c: 430 - 501

## Overview
Reads a specified number of bytes from a GSSAPI connection, blocking and waiting for data as needed to fulfill the complete request.

## Definition


## Detailed Description
The  function is a blocking read helper used during GSSAPI transport setup. It ensures that exactly the requested number of bytes are read from the network connection, handling partial reads and temporary blocking conditions transparently.

The function operates in a loop, continuously attempting to read data using  until the full requested length is obtained. When the underlying socket would block or returns temporary errors, it uses  to efficiently wait for the socket to become readable again.

This function is specifically designed for the GSSAPI handshake phase where complete messages must be received before processing can continue, making it different from the streaming approach used in .

## Parameters / Member Variables
- : Pointer to Port structure containing the connection state and socket information
- : Exact number of bytes to read from the connection

## Dependencies
- Functions called/Symbols referenced:
  - : Low-level function to read raw data from the socket
  - : PostgreSQL function to wait for socket readiness
  - : Global latch for the current process
- Global buffers used:
  - : Buffer where read data is stored
  - : Current amount of data in the receive buffer
- Constants used:
  - : Wait for socket to become readable
  - : Exit wait if postmaster dies
  - : Wait event type for monitoring
- Error codes handled:
  - , , : Retryable conditions
- Called from:
  - : During GSSAPI connection establishment

## Notes and Other Information
- This is a static function, only used within the GSSAPI backend module
- Always returns either -1 (permanent error) or the exact requested length
- Implements EOF detection by checking for zero bytes returned after waiting
- Uses PostgreSQL's latch mechanism for efficient waiting, allowing for clean shutdown
- Handles partial reads transparently, which can occur on slow or busy networks
- The function accumulates data in  starting from the current  position