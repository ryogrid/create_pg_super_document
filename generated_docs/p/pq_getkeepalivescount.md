# pq_getkeepalivescount

## Location
src/backend/libpq/pqcomm.c: 1798 - 1827

## Overview
Retrieves the TCP keep-alive probe count setting for a PostgreSQL connection port, determining how many unanswered keep-alive probes are sent before considering the connection dead.

## Definition
```c
int pq_getkeepalivescount(Port *port)
```

## Detailed Description
This function returns the TCP keep-alive count (TCP_KEEPCNT) for the specified port. The keep-alive count determines how many consecutive failed keep-alive probes are sent before the TCP connection is considered dead and closed. The function first checks if a custom value has been set for the port, and if not, retrieves and caches the system default value using getsockopt(). For Unix domain sockets or when TCP_KEEPCNT is not supported, it returns 0.

The function implements a caching mechanism to avoid repeated system calls - it stores the default system value in port->default_keepalives_count after the first retrieval.

## Parameters / Member Variables
- `port`: Pointer to the Port structure representing the client connection. If NULL or represents a Unix domain socket, the function returns 0.

## Dependencies
- Functions called/Symbols referenced:
  - getsockopt (system call to retrieve socket options)
  - ereport (PostgreSQL error reporting function)
  - errmsg (PostgreSQL error message formatting)
- Called from (representative examples):
  - pq_setkeepalivescount
  - show_tcp_keepalives_count

## Notes and Other Information
- The function is only functional when TCP_KEEPCNT is defined at compile time
- Returns 0 for Unix domain sockets since TCP keep-alive doesn't apply
- Uses a caching mechanism to store the default system value to avoid repeated getsockopt calls
- If getsockopt fails, it logs an error and sets the default to -1 to indicate unknown status
- The keep-alive count works in conjunction with keep-alive idle time and interval settings to manage connection health