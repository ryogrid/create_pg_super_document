# error_severity

## Location
[src/backend/utils/error/elog.c:3667-3718](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L3667-L3718)

## Overview
A utility function that converts PostgreSQL error level constants into their corresponding string representations for use in error reporting and logging.

## Definition
```c
const char *error_severity(int elevel)
```

## Detailed Description
This function serves as a central mapping utility that translates PostgreSQL's internal error level constants into human-readable string representations. It uses a switch statement to map each error level to its corresponding severity string. The function marks all severity strings for translation using gettext_noop(), allowing callers to localize the returned strings as needed. This design separates the string lookup from localization concerns, providing flexibility for different output contexts (localized client messages vs. non-localized log entries). The function handles all standard PostgreSQL error levels from DEBUG (lowest) to PANIC (highest), with a fallback for unknown levels.

## Parameters / Member Variables
- `elevel`: Integer constant representing the PostgreSQL error level (DEBUG1-5, LOG, INFO, NOTICE, WARNING, ERROR, FATAL, PANIC)

## Dependencies
- Functions called/Symbols referenced:
  - gettext_noop (for marking strings for translation)
  - DEBUG1, DEBUG2, DEBUG3, DEBUG4, DEBUG5
  - LOG, LOG_SERVER_ONLY
  - INFO, NOTICE
  - WARNING, WARNING_CLIENT_ONLY  
  - ERROR, FATAL, PANIC
- Called from (representative examples):
  - [send_message_to_server_log](../s/send_message_to_server_log.md)
  - [send_message_to_frontend](../s/send_message_to_frontend.md)
  - [write_csvlog](../w/write_csvlog.md)
  - [write_jsonlog](../w/write_jsonlog.md)

## Notes and Other Information
This function is a fundamental building block of PostgreSQL's error reporting system used across multiple output formats (frontend messages, server logs, CSV logs, JSON logs). The use of gettext_noop() allows the same function to serve both localized client communication and non-localized logging needs. The strings are intentionally not localized within this function, giving callers control over whether to apply localization via the _() macro.