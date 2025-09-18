# send_cancellable_query_impl

## Location
src/test/modules/libpq_pipeline/libpq_pipeline.c: 175 - 205

## Overview
A test utility function that sends a long-running pg_sleep query and ensures it reaches active execution state, preparing it for reliable cancellation testing.

## Definition
```c
static void send_cancellable_query_impl(int line, PGconn *conn, PGconn *monitorConn)
```

## Detailed Description
The `send_cancellable_query_impl` function is a specialized testing utility designed for libpq pipeline cancellation tests. It performs a carefully orchestrated sequence to set up a cancellable query:

1. **Connection Synchronization**: First waits for the target connection to be in "idle" state to ensure any previous operations have completed
2. **Query Dispatch**: Sends a `pg_sleep()` query with a configurable timeout (default 180 seconds via `PG_TEST_TIMEOUT_DEFAULT` environment variable)
3. **Execution Confirmation**: Waits for the sleep query to actually begin execution (reaches "PgSleep" wait event)

This two-step synchronization ensures that subsequent cancellation requests will have a reliably running query to cancel, preventing race conditions common in concurrent testing scenarios. The function uses asynchronous query sending (`PQsendQueryParams`) to avoid blocking the calling thread.

## Parameters / Member Variables
- `line`: Source code line number where the function was called (for error reporting context)
- `conn`: PostgreSQL connection handle where the cancellable query will be executed
- `monitorConn`: Secondary connection used to monitor the first connection's state via pg_stat_activity

## Dependencies
- Functions called/Symbols referenced:
  - [wait_for_connection_state](../w/wait_for_connection_state.md)() - waits for specific connection states/events
  - [PQbackendPID](../P/PQbackendPID.md)() - get backend process ID for monitoring
  - [PQsendQueryParams](../P/PQsendQueryParams.md)() - send parameterized query asynchronously
  - [PQerrorMessage](../P/PQerrorMessage.md)() - get connection error messages
  - `getenv()` - read environment variables
  - `pg_fatal_impl()` - report fatal test errors
  - `INT4OID` - integer parameter type OID

- Called from (via macro):
  - `send_cancellable_query()` macro (used 6+ times in various test scenarios)
  - Multiple test functions that validate query cancellation behavior

## Notes and Other Information
- Part of the libpq pipeline testing framework, specifically for cancellation testing
- Uses environment variable `PG_TEST_TIMEOUT_DEFAULT` to configure sleep duration (defaults to 180 seconds)
- The two-stage waiting ensures reliable test timing: connection must be idle before sending, and query must be running before canceling
- Prevents race conditions where cancellation requests arrive before queries start executing
- The long sleep duration ensures sufficient time for cancellation testing without timeout issues
- Critical for validating libpq's query cancellation functionality in pipeline mode
- Accessed via macro that automatically provides line number for error context