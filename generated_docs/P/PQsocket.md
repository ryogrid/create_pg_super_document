# PQsocket

## Location
src/interfaces/libpq/fe-connect.c: 7185 - 7192

## Overview
Returns the file descriptor (socket) of the connection to the PostgreSQL server, enabling direct socket operations and integration with event loops.

## Definition


## Detailed Description
This function provides access to the underlying socket file descriptor used for the PostgreSQL connection. It returns the socket that can be used with system calls like select(), poll(), epoll(), or kqueue() for asynchronous I/O operations and event-driven programming.

The function handles platform differences between Unix and Windows socket representations. On Windows, socket values are unsigned and invalid sockets are represented by INVALID_SOCKET (~0), but for API consistency across platforms, this function returns -1 for invalid sockets on all platforms.

The socket descriptor allows applications to integrate PostgreSQL connections into their own event loops, enabling non-blocking operations and efficient handling of multiple connections.

## Parameters / Member Variables
- : A pointer to the PGconn structure representing the database connection. Must not be NULL for valid results.

## Dependencies
- Functions called/Symbols referenced:
  - PGINVALID_SOCKET (constant representing an invalid socket)
- Called from (representative examples):
  - [libpqrcv_connect](../l/libpqrcv_connect.md) (in replication walreceiver)
  - [StreamLogicalLog](../S/StreamLogicalLog.md) (in pg_recvlogical)
  - [CopyStreamPoll](../C/CopyStreamPoll.md) (in receivelog)
  - [threadRun](../t/threadRun.md) (in pgbench for connection monitoring)
  - [wait_until_connected](../w/wait_until_connected.md) (in psql)
  - [wait_on_slots](../w/wait_on_slots.md) (in parallel slot management)

## Notes and Other Information
- Returns -1 for invalid connections (NULL pointer or invalid socket)
- The socket can be used for monitoring connection readiness with system I/O multiplexing functions
- Essential for implementing asynchronous and non-blocking libpq applications
- Cross-platform compatibility ensured by returning -1 for invalid sockets on all systems
- The socket remains valid for the lifetime of the connection
- Commonly used in high-performance applications that need to handle multiple database connections efficiently
- Applications should not directly read from or write to this socket - use libpq functions for data transfer