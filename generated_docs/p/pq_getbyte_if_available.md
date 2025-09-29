# pq_getbyte_if_available

## Location
[src/backend/libpq/pqcomm.c:1003-1061](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L1003-L1061)

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
  - [socket_set_nonblocking](../s/socket_set_nonblocking.md)
  - [secure_read](../s/secure_read.md)
  - EAGAIN
  - EINTR
  - EWOULDBLOCK
  - COMMERROR
  - [errcode_for_socket_access](../e/errcode_for_socket_access.md)
- Called from (representative examples):
  - [ProcessRepliesIfAny](../P/ProcessRepliesIfAny.md)

## Notes and Other Information
- Returns 1 if a byte was successfully read, 0 if no data available, EOF on error
- Sets socket to non-blocking mode before attempting direct socket read
- Does not use the buffer refill mechanism (pq_recvbuf) for new data
- Essential for replication and other time-sensitive operations
- Handles the complexity of non-blocking socket operations including proper errno interpretation

## Simplified Source

```c
int pq_getbyte_if_available(unsigned char *c)
{
    int r;

    Assert(PqCommReadingMsg);

    // First check if we have data in the buffer
    if (PqRecvPointer < PqRecvLength) {
        *c = PqRecvBuffer[PqRecvPointer++];
        return 1;
    }

    // Set socket to non-blocking mode for immediate return
    socket_set_nonblocking(true);

    errno = 0;

    // Try to read one byte directly from socket
    r = secure_read(MyProcPort, c, 1);
    if (r < 0) {
        // Handle non-blocking socket conditions
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
            r = 0;  // No data available
        else {
            // Real error occurred
            if (errno != 0)
                ereport(COMMERROR,
                        (errcode_for_socket_access(),
                         errmsg("could not receive data from client: %m")));
            r = EOF;
        }
    }
    else if (r == 0) {
        // Connection closed by client
        r = EOF;
    }

    return r;
}
```