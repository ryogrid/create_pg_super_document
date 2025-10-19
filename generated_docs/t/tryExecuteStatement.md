# tryExecuteStatement

## Location
[src/bin/pgbench/pgbench.c:1516-1530](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L1516-L1530)

## Overview
Executes a SQL statement using PQexec() and logs errors without exiting, allowing pgbench to continue execution even when non-critical SQL operations fail.

## Definition
```c
static void tryExecuteStatement(PGconn *con, const char *sql)
```

## Detailed Description
The `tryExecuteStatement` function is a more permissive counterpart to `executeStatement` that executes SQL statements while gracefully handling failures. Unlike `executeStatement`, this function does not terminate the program when a SQL statement fails. Instead, it logs the error message and explicitly indicates that the error is being ignored and execution will continue. This approach is suitable for optional or cleanup operations where failure should not prevent pgbench from continuing its main functionality. The function still provides proper error reporting so users are aware of issues, but allows pgbench to proceed with its operations.

## Parameters / Member Variables
- `con`: PostgreSQL database connection handle (PGconn pointer)
- `sql`: Null-terminated string containing the SQL statement to execute

## Dependencies
- Functions called/Symbols referenced:
  - [PQexec](../P/PQexec.md) (executes SQL statement via libpq)
  - [PQresultStatus](../P/PQresultStatus.md) (checks result status)
  - [PQerrorMessage](../P/PQerrorMessage.md) (retrieves error message from connection)
  - [PQclear](../P/PQclear.md) (frees result memory)
  - pg_log_error (logs error message)
  - pg_log_error_detail (logs additional error details with continuation notice)
- Constants used:
  - PGRES_COMMAND_OK (successful command completion status)
- Called from (representative examples):
  - [main](../m/main.md) (multiple calls for optional operations during pgbench startup/cleanup)

## Notes and Other Information
- Designed for non-critical SQL operations where failure should not be fatal
- Explicitly indicates error handling policy with "ignoring this error and continuing anyway" message
- Provides a gentler alternative to executeStatement for optional operations
- Proper memory management with PQclear() regardless of success/failure
- Used primarily in pgbench's main function for cleanup and optional initialization tasks
- Located in src/bin/pgbench/pgbench.c:1516-1530 and complements the stricter executeStatement function

## Simplified Source

```c
static void tryExecuteStatement(PGconn *con, const char *sql) {
    // Execute SQL statement and log errors but continue on failure

    PGresult *res = PQexec(con, sql);

    // Check if command failed, but don't exit
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        // Log error but explicitly indicate continuation
        pg_log_error("%s", PQerrorMessage(con));
        pg_log_error_detail("(ignoring this error and continuing anyway)");
    }

    PQclear(res);  // Always clean up result
}
```

**Key Points:**
- More permissive alternative to executeStatement
- Logs errors but doesn't terminate the program
- Suitable for optional or cleanup operations where failure is acceptable
- Explicitly indicates that errors are being ignored