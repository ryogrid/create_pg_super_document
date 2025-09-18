# errmsg

## Location
[src/backend/utils/error/elog.c:1070-1091](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L1070-L1091)

## Overview
Sets the primary error message text for the current error in PostgreSQL's error reporting system, supporting printf-style formatting with special errno expansion.

## Definition
```c
int errmsg(const char *fmt, ...)
```

## Detailed Description
This function is a core component of PostgreSQL's error reporting infrastructure that sets the primary error message for the current error context. It accepts a format string and variable arguments similar to printf, but with an important extension: "%m" in the format string is automatically replaced with the error message corresponding to the caller's current errno value. The function handles memory management by switching to the error's associated memory context during message processing and properly manages recursion depth for nested error calls.

## Parameters / Member Variables
- `fmt`: Format string for the error message (supports printf-style escapes plus "%m" for errno messages)
- `...`: Variable arguments corresponding to format specifiers in fmt
- Return value: Always returns 0 (return value is not meaningful)

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (struct type for error information)
  - CHECK_STACK_DEPTH (macro for stack depth validation)
  - EVALUATE_MESSAGE (macro for message processing and formatting)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (memory context management)
- Called from (representative examples):
  - Used extensively throughout PostgreSQL codebase in error reporting
  - Typically called within ereport() macro constructs

## Notes and Other Information
- Special format specifier "%m" expands to strerror(errno) automatically
- No newline needed at end of format string - ereport adds it for output methods that need it
- Manages memory context switching to ensure message is allocated in correct context
- Increments and decrements recursion_depth to track nested error calls
- Sets message_id field in ErrorData structure to the format string
- Part of PostgreSQL's comprehensive error reporting framework alongside ereport, elog, etc.
- Located in src/backend/utils/error/elog.c:1070-1091