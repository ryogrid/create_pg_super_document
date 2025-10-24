# PQfn

## Location
[src/interfaces/libpq/fe-exec.c:2980-3041](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L2980-L3041)

## Overview
PQfn is a PostgreSQL libpq function that sends a function call to the PostgreSQL backend, allowing direct invocation of server-side functions with specified arguments and return value handling.

## Definition
```c
PGresult *PQfn(PGconn *conn, int fnid, int *result_buf, int *result_len, int result_is_int, const PQArgBlock *args, int nargs)
```

## Detailed Description
PQfn provides a mechanism to call PostgreSQL server-side functions directly from client applications. It takes a function OID, arguments in a structured format, and parameters for handling the return value. The function performs comprehensive validation including connection state checks, pipeline mode restrictions, and socket status verification before delegating to the internal pqFunctionCall3 function.

The function is incompatible with pipeline mode and requires the connection to be in an idle state. It handles error state management by clearing previous errors when starting a new query cycle, unless operating in pipeline mode where existing error states should be preserved.

## Parameters / Member Variables
- `conn`: Database connection object
- `fnid`: OID of the PostgreSQL function to be called
- `result_buf`: Pointer to buffer for storing the function result
- `result_len`: Pointer to integer that will receive the actual length of the result
- `result_is_int`: Flag indicating if the result should be treated as an integer (1) or not (0)
- `args`: Array of PQArgBlock structures containing function arguments
- `nargs`: Number of arguments in the args array

## Dependencies
- Functions called/Symbols referenced:
  - pqClearConnErrorState
  - pgHavePendingResult
  - [pqFunctionCall3](../p/pqFunctionCall3.md)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md)
- Types used:
  - PQArgBlock
  - PQ_PIPELINE_OFF
  - PGINVALID_SOCKET
  - PGASYNC_IDLE
- Called from (representative examples):
  - [lo_open](../l/lo_open.md), lo_close, lo_read, lo_write (large object functions)
  - [lo_create](../l/lo_create.md), lo_unlink, lo_lseek (large object operations)

## Notes and Other Information
- Not allowed in pipeline mode - will return NULL with an error message
- Requires connection to be in idle state with valid socket
- Primarily used by large object (lo_*) functions in libpq
- Returns PGresult with PGRES_COMMAND_OK on success, PGRES_FATAL_ERROR on backend error, or NULL on communication failure
- Handles error state management appropriately for query cycle boundaries
- Located in src/interfaces/libpq/fe-exec.c:2980-3041
- Essential for PostgreSQL's large object interface implementation

## Simplified Source

```c
PGresult *PQfn(PGconn *conn, int fnid, int *result_buf, int *result_len,
               int result_is_int, const PQArgBlock *args, int nargs) {
    // Initialize result length
    *result_len = 0;

    // Validate connection
    if (!conn)
        return NULL;

    // Clear error state if starting new query cycle (not in pipeline)
    if (conn->cmd_queue_head == NULL)
        pqClearConnErrorState(conn);

    // Check pipeline mode restriction
    if (conn->pipelineStatus != PQ_PIPELINE_OFF) {
        libpq_append_conn_error(conn, "%s not allowed in pipeline mode", "PQfn");
        return NULL;
    }

    // Validate connection state
    if (conn->sock == PGINVALID_SOCKET || conn->asyncStatus != PGASYNC_IDLE ||
        pgHavePendingResult(conn)) {
        libpq_append_conn_error(conn, "connection in wrong state");
        return NULL;
    }

    // Delegate to protocol 3 function call implementation
    return pqFunctionCall3(conn, fnid, result_buf, result_len,
                          result_is_int, args, nargs);
}
```