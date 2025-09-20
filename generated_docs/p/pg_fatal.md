# pg_fatal

## Location
[src/bin/pg_upgrade/util.c:270-283](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/util.c#L270-L283)

## Overview
pg_fatal is a convenience macro that logs an error message and immediately terminates the program, serving as PostgreSQL's standard mechanism for handling fatal errors in frontend utilities.

## Definition

```c
void
pg_fatal(const char *fmt,...)
```
## Detailed Description
pg_fatal is a macro defined in PostgreSQL's common logging framework that combines error logging with program termination. It serves as a convenient shortcut for situations where an error is so severe that the program cannot continue execution. The macro first logs the error message using pg_log_generic with PG_LOG_ERROR level and PG_LOG_PRIMARY part, then immediately calls exit(1) to terminate the program. This macro is widely used throughout PostgreSQL's frontend utilities for handling fatal errors such as invalid command-line arguments, file access failures, and other unrecoverable conditions.

## Parameters / Member Variables
- : Variable arguments passed to pg_log_generic, typically a printf-style format string followed by format arguments

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_generic (logs the error message)
  - PG_LOG_ERROR (error log level enum value)
  - PG_LOG_PRIMARY (primary message part enum value)
  - exit (standard library function for program termination)
- Called from (representative examples):
  - pg_resetwal (command-line argument validation)
  - Various frontend utilities for error handling
  - File operation error handling throughout frontend code

## Notes and Other Information
- The macro uses a do-while(0) construct to ensure proper statement semantics when used in conditional contexts
- Always exits with code 1 to indicate failure to the shell
- Part of PostgreSQL's unified logging framework defined in src/include/common/logging.h
- In some contexts (like pg_dump), there may be alternative definitions that call exit_nicely() instead of exit() directly for cleanup purposes
- Widely used across PostgreSQL frontend utilities for consistent fatal error handling
- Should be used for truly unrecoverable errors rather than warnings or recoverable conditions