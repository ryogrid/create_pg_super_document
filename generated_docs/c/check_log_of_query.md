# check_log_of_query

## Location
[src/backend/utils/error/elog.c:2728-2750](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L2728-L2750)

## Overview
Determines whether a query statement should be included in PostgreSQL log output based on error level, statement hiding flags, and query string availability.

## Definition

```c
bool
check_log_of_query(ErrorData *edata)
```
## Detailed Description
This function serves as a gate-keeper for query logging in PostgreSQL's error and logging system. It evaluates three critical conditions to determine if a query statement should be logged: (1) whether the error level meets the minimum threshold defined by log_min_error_statement, (2) whether the statement is explicitly marked to be hidden via the hide_stmt flag, and (3) whether there is actually a query string available for logging (debug_query_string is not NULL). The function returns true only when all conditions are favorable for logging the query, ensuring that sensitive or irrelevant queries are appropriately filtered from the logs.

## Parameters / Member Variables
- : Pointer to ErrorData structure containing error information including severity level and statement hiding flags

## Dependencies
- Functions called/Symbols referenced:
  - [ErrorData](../E/ErrorData.md) (error data structure type)
  - is_log_level_output (function to check log level threshold)
  - log_min_error_statement (global configuration variable)
  - debug_query_string (global variable containing current query string)
- Called from (representative examples):
  - [write_csvlog](../w/write_csvlog.md) (src/backend/utils/error/csvlog.c:199)
  - [send_message_to_server_log](../s/send_message_to_server_log.md) (src/backend/utils/error/elog.c:3279)
  - [write_jsonlog](../w/write_jsonlog.md) (src/backend/utils/error/jsonlog.c:244)

## Notes and Other Information
- Returns false if any of the three conditions fail: insufficient log level, hidden statement flag set, or missing query string
- Used across multiple logging formats (CSV, JSON, and standard server log) to ensure consistent query logging behavior
- Critical for maintaining security by respecting statement hiding preferences
- Part of PostgreSQL's comprehensive error reporting and logging infrastructure
- The log_min_error_statement configuration controls the minimum severity level for query logging

## Simplified Source

```c
// Simplified version of check_log_of_query
bool check_log_of_query(ErrorData *edata) {
    // Check if error level meets minimum threshold for statement logging
    if (!is_log_level_output(edata->elevel, log_min_error_statement))
        return false;

    // Skip logging if statement is marked as hidden
    if (edata->hide_stmt)
        return false;

    // Ensure query string is available for logging
    if (debug_query_string == NULL)
        return false;

    return true;
}
```

Key simplifications made:
- Added descriptive comments for each condition check
- Maintained the original three-gate logic structure
- Preserved all essential functionality without modifications
- Enhanced readability with clearer inline documentation