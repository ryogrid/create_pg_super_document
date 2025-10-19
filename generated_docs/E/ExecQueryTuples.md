# ExecQueryTuples

## Location
[src/bin/psql/common.c:826-901](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L826-L901)

## Overview
Executes each field value in a query result as individual SQL statements, implementing the \gexec psql meta-command functionality for dynamic SQL execution.

## Definition
static bool ExecQueryTuples(const PGresult *result)

## Detailed Description
This function implements the core logic for the \gexec psql meta-command, which treats each cell in a query result as a SQL statement to be executed. It iterates through all rows and columns of the result set, executing non-NULL values as SQL queries via SendQuery(). The function includes several safety mechanisms including recursion prevention (by temporarily disabling gexec_flag), cancellation support, and error handling with optional early termination based on the ON_ERROR_STOP setting.

Key behaviors:
- Processes results row by row, column by column
- Skips NULL values entirely
- Supports cancellation via cancel_pressed global flag
- Respects ECHO_ALL mode for query echoing
- Handles errors according to ON_ERROR_STOP setting
- Prevents infinite recursion by temporarily disabling gexec_flag

## Parameters / Member Variables
- result: A PGresult pointer containing the query execution results, where each cell value will be executed as a SQL statement

## Dependencies
- Functions called/Symbols referenced:
  - [PQntuples](../P/PQntuples.md) (gets number of rows in result)
  - [PQnfields](../P/PQnfields.md) (gets number of columns in result)
  - [PQgetisnull](../P/PQgetisnull.md) (checks if cell value is NULL)
  - [PQgetvalue](../P/PQgetvalue.md) (retrieves cell value as string)
  - [SendQuery](../S/SendQuery.md) (executes SQL statement)
  - puts (outputs query text in ECHO_ALL mode)
  - fflush (flushes stdout output)
- Global variables accessed:
  - pset.gexec_flag (recursion prevention flag)
  - pset.echo (echo mode setting)
  - pset.singlestep (single-step execution flag)
  - pset.on_error_stop (error handling setting)
  - cancel_pressed (cancellation flag)
  - PSQL_ECHO_ALL (echo mode constant)
- Called from:
  - [PrintQueryResult](../P/PrintQueryResult.md) (in src/bin/psql/common.c:1020)

## Notes and Other Information
- This is a static function internal to psql's common.c module
- The function temporarily disables gexec_flag to prevent infinite recursion if executed queries contain \gexec commands
- Memory management is handled by the underlying libpq functions
- The function supports early termination on cancellation or errors (when ON_ERROR_STOP is enabled)
- Empty cells (NULL values) are silently skipped rather than executed as empty queries
- [Query](../Q/Query.md) echoing behavior depends on the current ECHO mode and single-step settings

## Simplified Source

```c
static bool ExecQueryTuples(const PGresult *result) {
    bool success = true;
    int nrows = PQntuples(result);
    int ncolumns = PQnfields(result);

    // Prevent infinite recursion
    pset.gexec_flag = false;

    // Execute each cell value as a SQL statement
    for (int r = 0; r < nrows; r++) {
        for (int c = 0; c < ncolumns; c++) {
            if (!PQgetisnull(result, r, c)) {
                const char *query = PQgetvalue(result, r, c);

                // Check for cancellation
                if (cancel_pressed)
                    goto loop_exit;

                // Echo query if ECHO_ALL mode is enabled
                if (pset.echo == PSQL_ECHO_ALL && !pset.singlestep) {
                    puts(query);
                    fflush(stdout);
                }

                // Execute the query
                if (!SendQuery(query)) {
                    success = false;
                    if (pset.on_error_stop)
                        goto loop_exit;
                }
            }
        }
    }

loop_exit:
    // Restore gexec_flag state
    pset.gexec_flag = true;

    return success;
}
```

This simplified version preserves the essential functionality: iterate through result cells, execute non-NULL values as SQL statements, handle cancellation and errors, prevent recursion, and support query echoing.