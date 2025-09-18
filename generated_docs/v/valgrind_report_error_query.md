# valgrind_report_error_query

## Location
[src/backend/tcop/postgres.c:216-228](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L216-L228)

## Overview
This is a debugging function that reports Valgrind memory errors associated with a specific SQL query during PostgreSQL execution.

## Definition


## Detailed Description
The  function is a diagnostic utility specifically designed for debugging memory-related issues in PostgreSQL when running under Valgrind memory analysis tool. It compares the current Valgrind error count with a previously stored count () and if new errors have been detected since the last check, it outputs a message identifying the SQL query that was being executed when the errors occurred. This helps developers correlate memory errors with specific database operations during development and testing phases.

The function uses Valgrind's built-in macros  to get the current error count and  to output diagnostic information. It only reports if there are new errors and a valid query string is provided.

## Parameters / Member Variables
- : A C string containing the SQL query text that was being processed when potential memory errors occurred. If NULL, no report is generated even if errors are detected.

## Dependencies
- Functions called/Symbols referenced:
  - VALGRIND_COUNT_ERRORS (Valgrind macro)
  - VALGRIND_PRINTF (Valgrind macro)
  - old_valgrind_error_count (global variable)
  - unlikely() (compiler hint macro)

- Called from (representative examples):
  - [exec_bind_message](../e/exec_bind_message.md) (src/backend/tcop/postgres.c:2090)
  - [exec_execute_message](../e/exec_execute_message.md) (src/backend/tcop/postgres.c:2355)
  - [PostgresMain](../P/PostgresMain.md) (multiple locations in src/backend/tcop/postgres.c)

## Notes and Other Information
- This function is conditionally compiled and only active when PostgreSQL is built with Valgrind support
- The function is static, meaning it's only accessible within the postgres.c compilation unit
- It's typically called at the end of message processing to check if any memory errors occurred during query execution
- The function is purely diagnostic and doesn't affect normal PostgreSQL operation
- It relies on the global variable  being properly maintained by other parts of the system