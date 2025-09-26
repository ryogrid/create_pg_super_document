# internal_putbytes

## Location
src/backend/libpq/pqcomm.c: 1276 - 1323

## Overview
A static inline function that efficiently buffers and sends data bytes to a PostgreSQL client connection with smart buffering logic and automatic flushing.

## Definition


## Detailed Description
The  function is an internal utility for efficiently sending data to PostgreSQL clients. It implements intelligent buffering logic that optimizes performance by:

1. Buffering small writes to reduce system calls
2. Bypassing the buffer for large writes when the buffer is empty
3. Automatically flushing the buffer when it becomes full
4. Handling partial writes and ensuring all data is eventually sent

The function uses PostgreSQL's global send buffer () and maintains buffer state through  and  variables. It switches the socket to blocking mode before flushing to ensure reliable data transmission.

## Parameters / Member Variables
- : Pointer to the data bytes to be sent
- : Number of bytes to send from the data buffer

## Dependencies
- Functions called/Symbols referenced:
  - socket_set_nonblocking (to set socket to blocking mode)
  - internal_flush (to flush the internal send buffer)
  - internal_flush_buffer (to flush data directly without buffering)
  - memcpy (to copy data into the send buffer)
- Called from (representative examples):
  - socket_putmessage (main message sending function)
  - pq_putmessage_v2 (version 2 protocol message sending)

## Notes and Other Information
- Function is marked as static inline for performance optimization
- Uses intelligent buffering: bypasses buffer for large writes when buffer is empty
- Automatically flushes buffer when full to prevent overflow
- Switches socket to blocking mode before flush operations to ensure reliable transmission
- Returns 0 on success, EOF on transmission errors
- Manages global buffer state variables (PqSendPointer, PqSendStart, PqSendBufferSize)
- Part of PostgreSQL's layered communication architecture for optimal network performance