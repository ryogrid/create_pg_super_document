# pqConnectDBComplete

## Location
[src/interfaces/libpq/fe-connect.c:2470-2595](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L2470-L2595)

## Overview
`pqConnectDBComplete` is an internal libpq function that blocks and completes a PostgreSQL database connection, handling the final stages of connection establishment with timeout support.

## Definition
```c
int pqConnectDBComplete(PGconn *conn)
```

## Detailed Description
This function implements a blocking connection completion mechanism that repeatedly polls the connection state using `PQconnectPoll()` until the connection is fully established or fails. It handles connection timeouts by parsing the `connect_timeout` parameter and managing timing for each host/address attempt. The function uses a state machine approach, waiting for socket readiness (reading or writing) and advancing the connection process through polling. If a timeout occurs for a specific host/address combination, it moves to the next available server. The function also supports connection cancellation requests through `PQcancelPoll()`.

## Parameters / Member Variables
- `conn`: Pointer to the PGconn structure representing the database connection being completed

## Dependencies
- Functions called/Symbols referenced:
  - [pqParseIntParam](pqParseIntParam.md): Parses the connect_timeout parameter
  - [PQgetCurrentTimeUSec](../P/PQgetCurrentTimeUSec.md): Gets current time for timeout calculations
  - [pqWaitTimed](pqWaitTimed.md): Waits for socket readiness with timeout
  - [PQconnectPoll](../P/PQconnectPoll.md): Advances the connection state machine
  - [PQcancelPoll](../P/PQcancelPoll.md): Handles connection cancellation requests
  - Connection status constants: `CONNECTION_BAD`, `CONNECTION_NEEDED`
  - Polling status constants: `PGRES_POLLING_OK`, `PGRES_POLLING_READING`, `PGRES_POLLING_WRITING`

- Called from (representative examples):
  - [PQconnectdbParams](../P/PQconnectdbParams.md): Main connection function with parameters
  - [PQconnectdb](../P/PQconnectdb.md): Simple connection function
  - `[PQsetdbLogin](../P/PQsetdbLogin.md)`: Legacy connection function
  - [PQreset](../P/PQreset.md): Connection reset function
  - [internal_ping](../i/internal_ping.md): Internal connection ping function
  - [PQcancelBlocking](../P/PQcancelBlocking.md): Blocking connection cancellation

## Notes and Other Information
This function is part of the internal libpq connection establishment process and should not be called directly by application code. It implements the blocking variant of connection completion, while `PQconnectPoll()` provides the non-blocking alternative. The function handles multiple host/address combinations and implements per-host timeout logic, allowing clients to fail over to alternative servers when connection attempts time out. The function returns 1 on successful connection establishment and 0 on failure, with detailed error information stored in the PGconn structure.

## Simplified Source

```c
int pqConnectDBComplete(PGconn *conn) {
    PostgresPollingStatusType flag = PGRES_POLLING_WRITING;
    pg_usec_time_t end_time = -1;
    int timeout = 0;

    // Validate connection
    if (conn == NULL || conn->status == CONNECTION_BAD)
        return 0;

    // Parse timeout if specified
    if (conn->connect_timeout != NULL) {
        if (!pqParseIntParam(conn->connect_timeout, &timeout, conn, "connect_timeout")) {
            conn->status = CONNECTION_BAD;
            return 0;
        }
    }

    for (;;) {
        // Set timeout for current host/address if changed
        if (flag != PGRES_POLLING_OK && timeout > 0) {
            end_time = PQgetCurrentTimeUSec() + timeout * 1000000;
        }

        // Wait for socket readiness based on polling state
        switch (flag) {
            case PGRES_POLLING_OK:
                return 1;  // Success!

            case PGRES_POLLING_READING:
                if (pqWaitTimed(1, 0, conn, end_time) == -1) {
                    conn->status = CONNECTION_BAD;
                    return 0;
                }
                break;

            case PGRES_POLLING_WRITING:
                if (pqWaitTimed(0, 1, conn, end_time) == -1) {
                    conn->status = CONNECTION_BAD;
                    return 0;
                }
                break;

            default:
                conn->status = CONNECTION_BAD;
                return 0;
        }

        // Handle timeout - try next server
        if (timeout_occurred) {
            conn->try_next_addr = true;
            conn->status = CONNECTION_NEEDED;
        }

        // Advance connection state machine
        if (conn->cancelRequest)
            flag = PQcancelPoll((PGcancelConn *) conn);
        else
            flag = PQconnectPoll(conn);
    }
}
```