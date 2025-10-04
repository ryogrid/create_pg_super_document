# PQgetCancel

## Location
[src/interfaces/libpq/fe-cancel.c:350-417](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-cancel.c#L350-L417)

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
  - [pqParseIntParam](../p/pqParseIntParam.md)
  - [PGcancel](PGcancel.md) (type)
  - [SockAddr](../S/SockAddr.md) (type)
  - PGINVALID_SOCKET (constant)
- Called from (representative examples):
  - [PQrequestCancel](PQrequestCancel.md) (in fe-cancel.c)
  - [SetCancelConn](../S/SetCancelConn.md) (in cancel utility functions)
  - [set_archive_cancel_info](../s/set_archive_cancel_info.md) (in pg_dump parallel operations)
  - [test_cancel](../t/test_cancel.md) (in libpq_pipeline test module)

## Notes and Other Information
- Returns NULL if the connection is invalid or lacks a valid socket
- Allocates memory for the PGcancel structure that must be freed with PQfreeCancel
- Copies critical cancellation data: remote address (raddr), backend PID (be_pid), and backend key (be_key)
- Initializes TCP parameters to -1 (unset) and then parses actual values from connection parameters
- Handles TCP keepalive settings: user_timeout, keepalives, keepalives_idle, keepalives_interval, and keepalives_count
- Thread-safe design allows cancellation from different threads than the one executing the query
- Essential for implementing responsive user interfaces that can cancel long-running queries
- The returned PGcancel object is independent of the original connection and remains valid even if the connection is closed

## Simplified Source

```c
PGcancel *PQgetCancel(PGconn *conn) {
    PGcancel *cancel;

    // Validate connection
    if (!conn || conn->sock == PGINVALID_SOCKET)
        return NULL;

    // Allocate cancel structure
    cancel = malloc(sizeof(PGcancel));
    if (cancel == NULL)
        return NULL;

    // Copy essential cancellation data
    memcpy(&cancel->raddr, &conn->raddr, sizeof(SockAddr));
    cancel->be_pid = conn->be_pid;
    cancel->be_key = conn->be_key;

    // Initialize TCP parameters to unset (-1)
    cancel->pgtcp_user_timeout = -1;
    cancel->keepalives = -1;
    cancel->keepalives_idle = -1;
    cancel->keepalives_interval = -1;
    cancel->keepalives_count = -1;

    // Parse TCP parameters from connection if available
    if (conn->pgtcp_user_timeout != NULL) {
        if (!pqParseIntParam(conn->pgtcp_user_timeout, &cancel->pgtcp_user_timeout, conn, "tcp_user_timeout"))
            goto fail;
    }
    if (conn->keepalives != NULL) {
        if (!pqParseIntParam(conn->keepalives, &cancel->keepalives, conn, "keepalives"))
            goto fail;
    }
    // ... similar parsing for other keepalive parameters ...

    return cancel;

fail:
    free(cancel);
    return NULL;
}
```