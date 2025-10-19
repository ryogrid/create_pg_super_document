# PQbackendPID

## Location
[src/interfaces/libpq/fe-connect.c:7193-7200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L7193-L7200)

## Overview
Returns the process ID (PID) of the PostgreSQL backend server process that is handling the current connection.

## Definition

```c
int
PQbackendPID(const PGconn *conn)
```
## Detailed Description
This function retrieves the process identifier of the PostgreSQL server backend process that is handling the specified connection. Each client connection to PostgreSQL is served by a dedicated backend process, and this function provides access to that process's PID.

The backend PID is useful for several purposes:
- Identifying which server process is handling a connection in system monitoring tools
- Correlating client activity with server-side logs and process information
- Advanced debugging and performance analysis
- Query cancellation operations (though PQcancel should be used for that purpose)

The function only returns a valid PID when the connection is in CONNECTION_OK status, ensuring that the backend process is actually established and ready.

## Parameters / Member Variables
- `*conn`: A pointer to the PGconn structure representing the database connection. Must not be NULL and should be in CONNECTION_OK status for valid results.
## Dependencies
- Functions called/Symbols referenced:
  - CONNECTION_OK (connection status constant)
- Called from (representative examples):
  - [libpqrcv_get_backend_pid](../l/libpqrcv_get_backend_pid.md) (in replication walreceiver)
  - [StartLogStreamer](../S/StartLogStreamer.md) (in pg_basebackup)
  - MAX_PROMPT_SIZE (in psql prompt handling)
  - [send_cancellable_query_impl](../s/send_cancellable_query_impl.md) (in pipeline testing)

## Notes and Other Information
- Returns 0 for invalid connections (NULL pointer or not CONNECTION_OK status)
- The backend PID is established during connection setup and remains constant for the connection lifetime
- Each connection gets its own unique backend process in PostgreSQL's process-per-connection model
- Useful for monitoring and debugging, especially in multi-connection scenarios
- The PID can be used to identify the corresponding backend process in system process lists and PostgreSQL's pg_stat_activity view
- Should not be used directly for process management - use appropriate libpq functions instead

## Simplified Source

```c
int PQbackendPID(const PGconn *conn) {
    // Safety check: must have valid connection in OK status
    if (!conn || conn->status != CONNECTION_OK)
        return 0;

    // Return the backend process ID
    return conn->be_pid;
}
```