# check_log_of_query

## Location
src/backend/utils/error/elog.c: 2728 - 2750

## Overview
Determines whether a query statement should be included in PostgreSQL log output based on error level, statement hiding flags, and query string availability.

## Definition


## Detailed Description
This function serves as a gate-keeper for query logging in PostgreSQL's error and logging system. It evaluates three critical conditions to determine if a query statement should be logged: (1) whether the error level meets the minimum threshold defined by log_min_error_statement, (2) whether the statement is explicitly marked to be hidden via the hide_stmt flag, and (3) whether there is actually a query string available for logging (debug_query_string is not NULL). The function returns true only when all conditions are favorable for logging the query, ensuring that sensitive or irrelevant queries are appropriately filtered from the logs.

## Parameters / Member Variables
- : Pointer to ErrorData structure containing error information including severity level and statement hiding flags

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (error data structure type)
  - is_log_level_output (function to check log level threshold)
  - log_min_error_statement (global configuration variable)
  - debug_query_string (global variable containing current query string)
- Called from (representative examples):
  - write_csvlog (src/backend/utils/error/csvlog.c:199)
  - send_message_to_server_log (src/backend/utils/error/elog.c:3279)
  - write_jsonlog (src/backend/utils/error/jsonlog.c:244)

## Notes and Other Information
- Returns false if any of the three conditions fail: insufficient log level, hidden statement flag set, or missing query string
- Used across multiple logging formats (CSV, JSON, and standard server log) to ensure consistent query logging behavior
- Critical for maintaining security by respecting statement hiding preferences
- Part of PostgreSQL's comprehensive error reporting and logging infrastructure
- The log_min_error_statement configuration controls the minimum severity level for query logging