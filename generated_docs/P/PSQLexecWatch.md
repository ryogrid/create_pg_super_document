# PSQLexecWatch

## Location
[src/bin/psql/common.c:675-704](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L675-L704)

## Overview
PSQLexecWatch is a specialized function in psql designed to execute queries for the \watch command, providing timing information and proper result processing for repeated query execution.

## Definition

```c
int
PSQLexecWatch(const char *query, const printQueryOpt *opt, FILE *printQueryFout, int min_rows)
```
## Detailed Description
PSQLexecWatch is specifically designed to support psql's \watch command functionality. It executes a query and processes its results with the following characteristics:

- Executes the query through ExecQueryAndProcessResults() with full result processing
- Provides timing information when timing is enabled (pset.timing)
- Uses PrintTiming() to display elapsed execution time
- Supports customizable output formatting through printQueryOpt parameter
- Allows output redirection to specified file stream
- Implements proper cancellation handling for interactive use
- Returns status codes to indicate execution success, repeatability, or error conditions

The function is optimized for repeated execution scenarios and provides detailed control over result display and formatting, making it ideal for monitoring queries that need to be run periodically.

## Parameters / Member Variables
- `*query`: The SQL query string to be executed
- `*opt`: Pointer to printQueryOpt structure containing formatting options for query results
- `*printQueryFout`: FILE pointer for output destination (can be different from stdout)
- `min_rows`: Minimum number of rows threshold for result processing behavior
## Dependencies
- Functions called/Symbols referenced:
  - [printQueryOpt](../p/printQueryOpt.md) (structure type for formatting options)
  - [SetCancelConn](../S/SetCancelConn.md) (sets up query cancellation handling)
  - [ExecQueryAndProcessResults](../E/ExecQueryAndProcessResults.md) (main query execution and result processing)
  - [ResetCancelConn](../R/ResetCancelConn.md) (cleans up cancellation setup)
  - [PrintTiming](PrintTiming.md) (displays timing information)

- Called from (representative examples):
  - [do_watch](../d/do_watch.md) (implements the \watch command functionality)

## Notes and Other Information
- Returns 1 on successful execution, 0 if query cannot be repeated (e.g., due to interrupt), -1 on error
- Timing display is conditional on pset.timing setting
- Validates database connection before proceeding with query execution
- Designed specifically for the \watch command's repeated execution model
- Supports output redirection separate from standard query execution
- Integrates cancellation handling to allow graceful interruption during watch operations

## Simplified Source

```c
int PSQLexecWatch(const char *query, const printQueryOpt *opt, FILE *printQueryFout, int min_rows) {
    bool timing = pset.timing;
    double elapsed_msec = 0;
    int res;

    // Check database connection
    if (!pset.db) {
        pg_log_error("You are currently not connected to a database.");
        return 0;
    }

    // Set up cancellation and execute query with full result processing
    SetCancelConn(pset.db);
    res = ExecQueryAndProcessResults(query, &elapsed_msec, NULL, true, min_rows, opt, printQueryFout);
    ResetCancelConn();

    // Display timing information if enabled
    if (timing)
        PrintTiming(elapsed_msec);

    return res;
}
```

This simplified version preserves the essential functionality: connection validation, query execution with timing measurement, cancellation handling, and conditional timing display for the \watch command.