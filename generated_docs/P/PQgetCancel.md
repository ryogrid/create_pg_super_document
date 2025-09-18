# PQgetCancel

## Location
src/interfaces/libpq/fe-cancel.c: 350 - 417

## Overview
Creates a thread-safe copy of cancellation information from a PostgreSQL connection, enabling query cancellation from different threads or processes.

## Definition
```c
PGcancel *PQgetCancel(PGconn *conn)
```

## Detailed Description
PQgetCancel extracts essential cancellation parameters from an active PostgreSQL connection and creates an independent PGcancel structure. This function is crucial for implementing thread-safe query cancellation, as it provides a snapshot of the connection's cancellation data that can be safely used from different threads without requiring locks on the original connection. The function copies the remote address, backend process ID, and secret key needed for cancellation, along with various TCP keepalive parameters that affect the cancellation connection's behavior.

## Parameters / Member Variables
- `conn`: A pointer to an active PGconn structure from which to extract cancellation information

## Dependencies
- Functions called/Symbols referenced:
  - malloc
  - memcpy  
  - pqParseIntParam
  - PGcancel (type)
  - SockAddr (type)
  - PGINVALID_SOCKET (constant)
- Called from (representative examples):
  - PQrequestCancel (in fe-cancel.c)
  - SetCancelConn (in cancel utility functions)
  - set_archive_cancel_info (in pg_dump parallel operations)
  - test_cancel (in libpq_pipeline test module)

## Notes and Other Information
- Returns NULL if the connection is invalid or lacks a valid socket
- Allocates memory for the PGcancel structure that must be freed with PQfreeCancel
- Copies critical cancellation data: remote address (raddr), backend PID (be_pid), and backend key (be_key)
- Initializes TCP parameters to -1 (unset) and then parses actual values from connection parameters
- Handles TCP keepalive settings: user_timeout, keepalives, keepalives_idle, keepalives_interval, and keepalives_count
- Thread-safe design allows cancellation from different threads than the one executing the query
- Essential for implementing responsive user interfaces that can cancel long-running queries
- The returned PGcancel object is independent of the original connection and remains valid even if the connection is closed