# PQcancelCreate

## Location
[src/interfaces/libpq/fe-cancel.c:65-171](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-cancel.c#L65-L171)

## Overview
Creates and returns a PGcancelConn structure that can be used to securely cancel a query on the given connection, setting up all necessary connection parameters and authentication tokens.

## Definition

```c
PGcancelConn *
PQcancelCreate(PGconn *conn)
```
## Detailed Description
PQcancelCreate creates a new cancel connection object by duplicating essential connection information from an active PostgreSQL connection. This function performs several critical steps:

1. Creates an empty PGconn structure using pqMakeEmptyPGconn()
2. Validates that the input connection is valid and open
3. Copies connection configuration and options from the original connection
4. Extracts and stores the backend process ID and cancellation key
5. Creates a single-host connection target pointing to the exact server address used by the original connection
6. Sets up addressing information to ensure the cancel request reaches the correct server

The function is designed to work with PostgreSQL's secure query cancellation mechanism, which requires proper authentication tokens (backend PID and key) to prevent unauthorized query cancellations. The resulting PGcancelConn must be used with either the blocking PQcancelBlocking() or the non-blocking PQcancelStart()/PQcancelPoll() workflow.

## Parameters / Member Variables
- `*conn`: Pointer to an active PGconn connection from which to extract cancellation information
## Dependencies
- Functions called/Symbols referenced:
  - [pqMakeEmptyPGconn](../p/pqMakeEmptyPGconn.md)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
  - [pqCopyPGconn](../p/pqCopyPGconn.md)
  - [pqConnectOptions2](../p/pqConnectOptions2.md)
  - [pqReleaseConnHosts](../p/pqReleaseConnHosts.md)
  - calloc
  - strdup
- Called from (representative examples):
  - [disconnectDatabase](../d/disconnectDatabase.md) (src/fe_utils/connect_utils.c:164)
  - [libpqsrv_cancel](../l/libpqsrv_cancel.md) (src/include/libpq/libpq-be-fe-helpers.h:391)
  - [test_cancel](../t/test_cancel.md) (src/test/modules/libpq_pipeline/libpq_pipeline.c:287)

## Notes and Other Information
- Returns NULL only if the initial pqMakeEmptyPGconn() call fails due to memory allocation issues
- Even on error conditions (invalid connection, closed socket, etc.), returns a PGcancelConn with error state rather than NULL
- The cancel connection is restricted to connect only to the exact host/address of the original connection, not all possible hosts
- Memory allocation failures during host information copying result in an out-of-memory error state
- The cancelRequest flag is set to true to indicate this connection is specifically for cancellation purposes
- Connection status is set to CONNECTION_ALLOCATED on success, CONNECTION_BAD on critical errors

## Simplified Source

```c
PGcancelConn *
PQcancelCreate(PGconn *conn)
{
    PGconn *cancelConn = pqMakeEmptyPGconn();
    if (!cancelConn) {
        return NULL;
    }

    // Validate input connection
    if (!conn || conn->sock == PGINVALID_SOCKET) {
        libpq_append_conn_error(cancelConn, "invalid or closed connection");
        return (PGcancelConn *) cancelConn;
    }

    // Set up as cancel connection and copy connection settings
    cancelConn->cancelRequest = true;
    if (!pqCopyPGconn(conn, cancelConn) || !pqConnectOptions2(cancelConn)) {
        return (PGcancelConn *) cancelConn;
    }

    // Copy cancellation tokens from original connection
    cancelConn->be_pid = conn->be_pid;
    cancelConn->be_key = conn->be_key;

    // Set up single-host connection to exact server address
    pqReleaseConnHosts(cancelConn);
    cancelConn->nconnhost = 1;
    cancelConn->naddr = 1;

    // Allocate and copy host information
    cancelConn->connhost = calloc(1, sizeof(pg_conn_host));
    cancelConn->addr = calloc(1, sizeof(AddrInfo));
    if (!cancelConn->connhost || !cancelConn->addr) {
        goto oom_error;
    }

    // Copy host details from original connection
    pg_conn_host originalHost = conn->connhost[conn->whichhost];
    if (originalHost.host) {
        cancelConn->connhost[0].host = strdup(originalHost.host);
    }
    if (originalHost.port) {
        cancelConn->connhost[0].port = strdup(originalHost.port);
    }
    // Copy other host fields as needed...

    // Copy address information
    cancelConn->addr[0].addr = conn->raddr;
    cancelConn->addr[0].family = conn->raddr.addr.ss_family;

    cancelConn->status = CONNECTION_ALLOCATED;
    return (PGcancelConn *) cancelConn;

oom_error:
    cancelConn->status = CONNECTION_BAD;
    libpq_append_conn_error(cancelConn, "out of memory");
    return (PGcancelConn *) cancelConn;
}
```