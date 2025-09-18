# pq_recvbuf

## Location
src/backend/libpq/pqcomm.c: 897 - 962

## Overview
Loads bytes from the client connection into the input buffer for subsequent processing by higher-level message parsing functions.

## Definition
static int pq_recvbuf(void)

## Detailed Description
pq_recvbuf is a low-level function responsible for filling the PostgreSQL receive buffer (PqRecvBuffer) with data from the client connection. The function first compacts any unread data in the buffer by moving it to the beginning, then uses secure_read to fetch new data from the socket. It operates in blocking mode and will continuously attempt to read data until successful or an error occurs. The function handles interrupts (EINTR) gracefully by retrying the read operation.

## Parameters / Member Variables
- No parameters (operates on global variables)

## Dependencies
- Functions called/Symbols referenced:
  - [socket_set_nonblocking](../s/socket_set_nonblocking.md)
  - [secure_read](../s/secure_read.md)
  - PQ_RECV_BUFFER_SIZE
  - EINTR
  - COMMERROR
  - [errcode_for_socket_access](../e/errcode_for_socket_access.md)
- Called from (representative examples):
  - [pq_getbyte](pq_getbyte.md)
  - [pq_peekbyte](pq_peekbyte.md)
  - [pq_getbytes](pq_getbytes.md)
  - [pq_discardbytes](pq_discardbytes.md)

## Notes and Other Information
- This is a static function, only accessible within pqcomm.c
- Operates on global variables PqRecvBuffer, PqRecvLength, and PqRecvPointer
- Always sets the socket to blocking mode before attempting to read
- Returns 0 on success, EOF on failure or connection close
- Critical for error handling: does not use ereport() that might write to client to avoid recursion