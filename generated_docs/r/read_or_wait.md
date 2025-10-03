# read_or_wait

## Location
[src/backend/libpq/be-secure-gssapi.c:430-501](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/be-secure-gssapi.c#L430-L501)

## Overview
Reads a specified number of bytes from a GSSAPI connection, blocking and waiting for data as needed to fulfill the complete request.

## Definition

```c
static ssize_t
read_or_wait(Port *port, ssize_t len)
```
## Detailed Description
The  function is a blocking read helper used during GSSAPI transport setup. It ensures that exactly the requested number of bytes are read from the network connection, handling partial reads and temporary blocking conditions transparently.

The function operates in a loop, continuously attempting to read data using  until the full requested length is obtained. When the underlying socket would block or returns temporary errors, it uses  to efficiently wait for the socket to become readable again.

This function is specifically designed for the GSSAPI handshake phase where complete messages must be received before processing can continue, making it different from the streaming approach used in .

## Parameters / Member Variables
- `*port`: Pointer to Port structure containing the connection state and socket information
- `len`: Exact number of bytes to read from the connection
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

## Simplified Source

```c
// Simplified version of read_or_wait
static ssize_t read_or_wait(Port *port, ssize_t len) {
    ssize_t ret;

    // Keep reading until we get the full requested length
    while (PqGSSRecvLength < len) {
        // Attempt to read remaining data
        ret = secure_raw_read(port, PqGSSRecvBuffer + PqGSSRecvLength,
                              len - PqGSSRecvLength);

        // Handle permanent errors (not retryable conditions)
        if (ret < 0 && !(errno == EWOULDBLOCK || errno == EAGAIN || errno == EINTR)) {
            return -1;
        }

        // Handle temporary blocking or EOF
        if (ret <= 0) {
            // Wait for socket to become readable
            WaitLatchOrSocket(MyLatch,
                              WL_SOCKET_READABLE | WL_EXIT_ON_PM_DEATH,
                              port->sock, 0, WAIT_EVENT_GSS_OPEN_SERVER);

            // Check for EOF by attempting another read
            if (ret == 0) {
                ret = secure_raw_read(port, PqGSSRecvBuffer + PqGSSRecvLength,
                                      len - PqGSSRecvLength);
                if (ret == 0) {
                    // Confirmed EOF - client disconnected
                    return -1;
                }
            }

            // If still negative, retry the loop
            if (ret < 0)
                continue;
        }

        // Update buffer length with successful read
        PqGSSRecvLength += ret;
    }

    return len;
}
```

Key simplifications made:
- Added explanatory comments for each phase of the read loop
- Clarified EOF detection logic with double-check approach
- Maintained essential non-blocking I/O handling
- Preserved error handling for retryable vs permanent errors
- Clear structure showing the accumulative reading approach