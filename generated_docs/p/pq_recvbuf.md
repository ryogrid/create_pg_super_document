# pq_recvbuf

## Location
[src/backend/libpq/pqcomm.c:897-962](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L897-L962)

## Overview
Loads bytes from the client connection into the input buffer for subsequent processing by higher-level message parsing functions.

## Definition
static int pq_recvbuf(void)

## Detailed Description
pq_recvbuf is a low-level function responsible for filling the PostgreSQL receive buffer (PqRecvBuffer) with data from the client connection. The function first compacts any unread data in the buffer by moving it to the beginning, then uses secure_read to fetch new data from the socket. It operates in blocking mode and will continuously attempt to read data until successful or an error occurs. The function handles interrupts (EINTR) gracefully by retrying the read operation.

## Parameters / Member Variables


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

## Simplified Source

```c
// Simplified version of pq_recvbuf
static int pq_recvbuf(void) {
    // Compact buffer by moving unread data to beginning
    if (PqRecvPointer > 0) {
        if (PqRecvLength > PqRecvPointer) {
            memmove(PqRecvBuffer, PqRecvBuffer + PqRecvPointer,
                    PqRecvLength - PqRecvPointer);
            PqRecvLength -= PqRecvPointer;
            PqRecvPointer = 0;
        } else {
            PqRecvLength = PqRecvPointer = 0;
        }
    }

    // Ensure blocking mode for reliable reads
    socket_set_nonblocking(false);

    // Read new data into buffer
    for (;;) {
        int r;
        errno = 0;

        r = secure_read(MyProcPort, PqRecvBuffer + PqRecvLength,
                        PQ_RECV_BUFFER_SIZE - PqRecvLength);

        if (r < 0) {
            // Retry on interrupt
            if (errno == EINTR)
                continue;

            // Log error only to postmaster (avoid client recursion)
            if (errno != 0)
                ereport(COMMERROR, (errcode_for_socket_access(),
                       errmsg("could not receive data from client: %m")));
            return EOF;
        }

        if (r == 0) {
            // EOF detected - connection closed
            return EOF;
        }

        // Update buffer length and return success
        PqRecvLength += r;
        return 0;
    }
}
```

Key simplifications made:
- Added explanatory comments for each major operation
- Simplified error handling logic
- Clarified buffer management operations
- Maintained essential networking and error handling logic