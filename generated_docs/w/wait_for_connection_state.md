# wait_for_connection_state

## Location
src/test/modules/libpq_pipeline/libpq_pipeline.c: 123 - 171

## Overview
A test utility function that polls the pg_stat_activity system view to wait for a database connection to reach a specific state or wait event.

## Definition
```c
static void wait_for_connection_state(int line, PGconn *monitorConn, int procpid, char *state, char *event)
```

## Detailed Description
The `wait_for_connection_state` function provides a synchronization mechanism for libpq pipeline testing by monitoring a specific database backend process until it reaches a desired state. It queries the `pg_stat_activity` system view repeatedly until the target condition is met.

The function supports two mutually exclusive monitoring modes:
- **State monitoring**: Wait for a connection to reach a specific state (e.g., "idle", "active")
- **Event monitoring**: Wait for a connection to be waiting on a specific event (e.g., "PgSleep")

The function uses parameterized queries to safely check the process status and implements a polling loop with 10ms intervals to avoid excessive system load. It ensures reliable synchronization in test scenarios where precise timing of database operations is critical.

## Parameters / Member Variables
- `line`: Source code line number where the function was called (for error reporting context)
- `monitorConn`: PostgreSQL connection handle used to query pg_stat_activity
- `procpid`: Process ID of the target backend to monitor
- `state`: Target connection state to wait for (mutually exclusive with event, one must be NULL)
- `event`: Target wait event to monitor (mutually exclusive with state, one must be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [PQexecParams](../P/PQexecParams.md)() - execute parameterized SQL queries
  - [PQresultStatus](../P/PQresultStatus.md)() - get query result status
  - [PQntuples](../P/PQntuples.md)() - get number of result rows
  - [PQnfields](../P/PQnfields.md)() - get number of result columns
  - [PQgetvalue](../P/PQgetvalue.md)() - extract result value
  - [PQclear](../P/PQclear.md)() - free result memory
  - [PQerrorMessage](../P/PQerrorMessage.md)() - get connection error message
  - [psprintf](../p/psprintf.md)() - formatted string allocation
  - [pfree](../p/pfree.md)() - memory deallocation
  - [pg_usleep](../p/pg_usleep.md)() - microsecond sleep function
  - `pg_fatal_impl()` - fatal error reporting
  - `PGRES_TUPLES_OK` - successful query result constant
  - `INT4OID`, `TEXTOID` - parameter type OIDs

- Called from:
  - [send_cancellable_query_impl](../s/send_cancellable_query_impl.md)() - waits for "idle" state before sending query
  - [send_cancellable_query_impl](../s/send_cancellable_query_impl.md)() - waits for "PgSleep" event after sending sleep query

## Notes and Other Information
- Part of the libpq pipeline testing framework, not used in production code
- Uses Assert() to enforce mutual exclusivity between state and event parameters
- Implements busy-waiting with 10ms intervals to balance responsiveness and system load
- Critical for reliable testing of query cancellation and pipeline operations
- Queries pg_stat_activity system view which requires appropriate database permissions
- The polling approach ensures tests wait for actual state changes rather than relying on fixed timeouts
- Commonly used to ensure queries are actually running before attempting cancellation