# socket_set_nonblocking

## Location
[src/backend/libpq/pqcomm.c:880-896](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L880-L896)

## Overview
Sets the socket blocking/non-blocking mode for the current client connection by updating the connection state flag.

## Definition

```c
static void
socket_set_nonblocking(bool nonblocking)
```
## Detailed Description
This function controls the blocking behavior of the current client connection socket by setting or clearing the noblock flag in the MyProcPort structure. Unlike typical socket configuration functions that directly manipulate socket options using system calls, this function works at the PostgreSQL protocol level by updating an internal flag that governs how I/O operations are performed.

The function requires an active client connection (MyProcPort must not be NULL) and will raise an error if called without an established connection. This is part of PostgreSQL's low-level I/O routines that handle communication between the backend and frontend clients.

## Parameters / Member Variables
- : Boolean flag indicating whether to set the socket to non-blocking (true) or blocking (false) mode

## Dependencies
- Functions called/Symbols referenced:
  - MyProcPort (global variable pointing to current client connection structure)
  - ereport/errmsg (PostgreSQL error reporting functions)
  - [errcode](../e/errcode.md) (PostgreSQL error code function)
  - ERRCODE_CONNECTION_DOES_NOT_EXIST (PostgreSQL error code constant)
  - ERROR (PostgreSQL error level constant)

- Called from (representative examples):
  - [pq_recvbuf](../p/pq_recvbuf.md) (when managing receive buffer operations)
  - [pq_getbyte_if_available](../p/pq_getbyte_if_available.md) (for non-blocking byte reading)
  - [internal_putbytes](../i/internal_putbytes.md) (during data transmission)
  - [socket_flush](socket_flush.md) (when flushing output buffers)
  - [socket_flush_if_writable](socket_flush_if_writable.md) (for conditional buffer flushing)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same source file (pqcomm.c)
- Part of the low-level I/O routines section that handles established client connections
- Does not directly call system socket functions but instead sets a flag that influences subsequent I/O operations
- Raises a CONNECTION_DOES_NOT_EXIST error if called without an active client connection
- The actual socket behavior change is implemented by other I/O functions that check the MyProcPort->noblock flag

## Simplified Source

```c
// Simplified version of socket_set_nonblocking
static void
socket_set_nonblocking(bool nonblocking)
{
    // Verify active client connection exists
    if (MyProcPort == NULL)
        ereport(ERROR, (errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST),
                       errmsg("there is no client connection")));

    // Set the non-blocking flag on current connection
    MyProcPort->noblock = nonblocking;
}
```

Key simplifications made:
- No simplifications needed - function is already very simple and clear
- Preserved essential error checking for connection validity
- Maintained single responsibility: setting the noblock flag
- Function logic is straightforward: validate connection then set flag