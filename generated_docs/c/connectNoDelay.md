# connectNoDelay

## Location
[src/interfaces/libpq/fe-connect.c:2034-2059](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L2034-L2059)

## Overview
Sets the TCP_NODELAY socket option to disable Nagle's algorithm for immediate packet transmission on the connection socket.

## Definition

```c
struct sockaddr_storage *addr = &conn->raddr.addr;
```
## Detailed Description
This static function configures a PostgreSQL connection socket to use TCP_NODELAY mode, which disables Nagle's algorithm. Nagle's algorithm buffers small packets to reduce network overhead, but this can introduce latency in interactive applications. By setting TCP_NODELAY, the function ensures that data is transmitted immediately without buffering delays, which is beneficial for database connections where low latency is important.

The function uses the setsockopt() system call to set the TCP_NODELAY option on the socket. If the operation fails, it logs an appropriate error message to the connection's error buffer. The function is conditionally compiled and only operates when TCP_NODELAY is available on the system.

## Parameters / Member Variables
- : Pointer to PGconn structure containing the socket to configure

## Dependencies
- Functions called/Symbols referenced:
  - setsockopt (system call)
  - libpq_append_conn_error
  - SOCK_STRERROR
  - SOCK_ERRNO
- Called from (representative examples):
  - Connection establishment code (referenced by CONNECTION_FAILED)

## Notes and Other Information
- Returns 1 on success, 0 on failure
- The function is static (internal to fe-connect.c)
- Conditionally compiled only when TCP_NODELAY is defined
- If TCP_NODELAY is not available, the function always returns 1 (success)
- The TCP_NODELAY option is applied at the IPPROTO_TCP level
- Error messages include system-specific socket error descriptions
- This optimization is particularly important for interactive database sessions where query response time matters more than network efficiency