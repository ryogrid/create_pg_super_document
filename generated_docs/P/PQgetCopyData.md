# PQgetCopyData

## Location
[src/interfaces/libpq/fe-exec.c:2816-2853](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L2816-L2853)

## Overview
Retrieves a row of data from the PostgreSQL backend during COPY OUT or COPY BOTH operations, providing the client-side interface for reading bulk data from the server.

## Definition

```c
int
PQgetCopyData(PGconn *conn, char **buffer, int async)
```
## Detailed Description
PQgetCopyData reads data rows from the PostgreSQL server during COPY OUT operations. It acts as a high-level wrapper around the protocol-specific pqGetCopyData3 function, handling connection state validation and providing a consistent interface for both synchronous and asynchronous operation modes.

The function allocates memory for received data and returns it through the buffer parameter. The caller is responsible for freeing the allocated memory using free() when the data is no longer needed. The function supports both blocking and non-blocking modes of operation based on the async parameter.

Key features:
- Validates connection state before attempting data retrieval
- Delegates to protocol-specific implementation (pqGetCopyData3)
- Handles memory allocation for received data rows
- Supports both synchronous and asynchronous operation modes
- Provides clear return codes for different completion states

## Parameters / Member Variables
- `*conn`: PostgreSQL connection handle that must be in COPY OUT or COPY BOTH state
- `**buffer`: Pointer to char* that will receive the address of the allocated data buffer
- `async`: Boolean flag controlling blocking behavior (1 for non-blocking, 0 for blocking)
## Dependencies
- Functions called/Symbols referenced:
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
  - [pqGetCopyData3](../p/pqGetCopyData3.md)
  - PGASYNC_COPY_OUT
  - PGASYNC_COPY_BOTH
- Called from (representative examples):
  - [handleCopyOut](../h/handleCopyOut.md) (psql)
  - [dumpTableData_copy](../d/dumpTableData_copy.md) (pg_dump)
  - [libpqrcv_receive](../l/libpqrcv_receive.md) (replication)
  - [ReceiveCopyData](../R/ReceiveCopyData.md) (pg_basebackup)

## Notes and Other Information
- Returns row length (> 0) on success with data in *buffer
- Returns 0 if no data available yet (only in async mode)
- Returns -1 on end of copy (check PQgetResult for final status)
- Returns -2 on error (check PQerrorMessage for details)
- The buffer is malloc'd by the function and must be freed by the caller
- Buffer is set to NULL in all failure cases for safety
- Used primarily in COPY TO STDOUT operations and replication contexts
- Critical component for efficient bulk data retrieval from PostgreSQL
- The async parameter enables integration with event-driven applications

## Simplified Source

```c
int PQgetCopyData(PGconn *conn, char **buffer, int async)
{
    // Initialize buffer to NULL for all failure cases
    *buffer = NULL;

    // Basic connection validation
    if (!conn)
        return -2;

    // Verify connection is in COPY OUT or COPY BOTH state
    if (conn->asyncStatus != PGASYNC_COPY_OUT &&
        conn->asyncStatus != PGASYNC_COPY_BOTH)
    {
        libpq_append_conn_error(conn, "no COPY in progress");
        return -2;
    }

    // Delegate to protocol version 3 implementation
    return pqGetCopyData3(conn, buffer, async);
}
```