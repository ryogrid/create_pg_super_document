# internal_putbytes

## Location
[src/backend/libpq/pqcomm.c:1276-1323](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L1276-L1323)

## Overview
A static inline function that efficiently buffers and sends data bytes to a PostgreSQL client connection with smart buffering logic and automatic flushing.

## Definition

```c
static inline int
internal_putbytes(const char *s, size_t len)
```
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
  - [socket_set_nonblocking](../s/socket_set_nonblocking.md) (to set socket to blocking mode)
  - [internal_flush](internal_flush.md) (to flush the internal send buffer)
  - [internal_flush_buffer](internal_flush_buffer.md) (to flush data directly without buffering)
  - memcpy (to copy data into the send buffer)
- Called from (representative examples):
  - [socket_putmessage](../s/socket_putmessage.md) (main message sending function)
  - [pq_putmessage_v2](../p/pq_putmessage_v2.md) (version 2 protocol message sending)

## Notes and Other Information
- Function is marked as static inline for performance optimization
- Uses intelligent buffering: bypasses buffer for large writes when buffer is empty
- Automatically flushes buffer when full to prevent overflow
- Switches socket to blocking mode before flush operations to ensure reliable transmission
- Returns 0 on success, EOF on transmission errors
- Manages global buffer state variables (PqSendPointer, PqSendStart, PqSendBufferSize)
- Part of PostgreSQL's layered communication architecture for optimal network performance

## Simplified Source

```c
// Simplified version of internal_putbytes
static inline int internal_putbytes(const char *s, size_t len) {
    while (len > 0) {
        // Flush buffer if it's full
        if (PqSendPointer >= PqSendBufferSize) {
            socket_set_nonblocking(false);
            if (internal_flush()) {
                return EOF;
            }
        }

        // For large data and empty buffer, send directly without buffering
        if (len >= PqSendBufferSize && PqSendStart == PqSendPointer) {
            size_t start = 0;
            socket_set_nonblocking(false);
            if (internal_flush_buffer(s, &start, &len)) {
                return EOF;
            }
        } else {
            // Copy data to buffer up to available space
            size_t amount = PqSendBufferSize - PqSendPointer;
            if (amount > len) {
                amount = len;
            }
            memcpy(PqSendBuffer + PqSendPointer, s, amount);
            PqSendPointer += amount;
            s += amount;
            len -= amount;
        }
    }

    return 0;  // Success
}
```

Key simplifications made:
- Preserved the intelligent buffering logic
- Maintained the three main strategies: flush when full, direct send for large data, buffer small data
- Added clear comments explaining each buffering decision
- Kept the error handling and socket management
- Streamlined the flow: check buffer → handle large data → buffer remaining data