# should_output_to_server

## Location
[src/backend/utils/error/elog.c:239-247](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L239-L247)

## Overview
Determines whether a message of a given error level should be output to the server log based on the current log_min_messages setting.

## Definition
```c
static inline bool should_output_to_server(int elevel)
```

## Detailed Description
This is a policy-setting subroutine that provides a centralized decision point for determining whether an error/log message should be written to the server log. It acts as a wrapper around the `is_log_level_output` function, comparing the provided error level against the `log_min_messages` configuration parameter. The function implements PostgreSQL's logging policy where LOG messages are treated specially in the severity hierarchy, sorting between ERROR and FATAL rather than following a simple numeric comparison.

## Parameters / Member Variables
- `elevel`: The error/message level to check (e.g., DEBUG, INFO, NOTICE, WARNING, ERROR, FATAL, PANIC, LOG)

## Dependencies
- Functions called/Symbols referenced:
  - is_log_level_output
  - log_min_messages (global variable)
- Called from (representative examples):
  - [message_level_is_interesting](../m/message_level_is_interesting.md)
  - [errstart](../e/errstart.md)
  - [pg_re_throw](../p/pg_re_throw.md)

## Notes and Other Information
- This function is declared as `static inline` for performance optimization since it's called frequently during error processing
- The function is part of PostgreSQL's centralized error logging policy system located in src/backend/utils/error/elog.c
- The decision logic handles the special case where LOG messages sort between ERROR and FATAL in the severity hierarchy, rather than using simple numeric comparison
- Used specifically for server-side logging decisions; client output decisions are handled by the separate `should_output_to_client` function