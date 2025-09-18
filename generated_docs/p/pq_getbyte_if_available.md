# pq_getbyte_if_available

## Location
src/backend/libpq/pqcomm.c: 1003 - 1061

## Overview
Attempts to read a single byte from the client connection in non-blocking mode, returning immediately if no data is available.

## Definition
int pq_getbyte_if_available(unsigned char *c)

## Detailed Description
pq_getbyte_if_available provides a non-blocking alternative to pq_getbyte for scenarios where the server cannot afford to wait for client data. It first checks if data is available in the existing buffer, and if not, attempts a non-blocking read directly from the socket. The function carefully handles various socket states including EAGAIN, EWOULDBLOCK, and EINTR conditions that are normal for non-blocking operations. This function is crucial for implementing responsive server behavior when dealing with potentially slow or unresponsive clients.

## Parameters / Member Variables
- : Pointer to unsigned char where the received byte will be stored

## Dependencies
- Functions called/Symbols referenced:
  - socket_set_nonblocking
  - secure_read
  - EAGAIN
  - EINTR
  - EWOULDBLOCK
  - COMMERROR
  - errcode_for_socket_access
- Called from (representative examples):
  - ProcessRepliesIfAny

## Notes and Other Information
- Returns 1 if a byte was successfully read, 0 if no data available, EOF on error
- Sets socket to non-blocking mode before attempting direct socket read
- Does not use the buffer refill mechanism (pq_recvbuf) for new data
- Essential for replication and other time-sensitive operations
- Handles the complexity of non-blocking socket operations including proper errno interpretation