# errmsg_internal

## Location
[src/backend/utils/error/elog.c:1157-1179](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L1157-L1179)

## Overview
Adds a primary error message text to the current error without translation, used for internal errors that are not intended for internationalization.

## Definition
```c
int errmsg_internal(const char *fmt, ...) pg_attribute_printf(1, 2);
```

## Detailed Description
`errmsg_internal` is exactly like `errmsg()` except that strings passed to it are not translated and are customarily left out of the internationalization message dictionary. This function is designed for two specific use cases:

1. **"Can't happen" cases**: Internal error conditions that are probably not worth spending translation effort on, as they indicate programming errors or extremely rare conditions.

2. **Translation-unsafe contexts**: Certain cases where translation must not be attempted because the translation would fail and result in infinite error recursion.

The function operates within PostgreSQL's error handling framework, setting up the message in the current error data structure. It manages memory context switching and recursion depth tracking to ensure safe operation even in error conditions.

## Parameters / Member Variables
- `fmt`: Format string for the error message (printf-style)
- `...`: Variable arguments corresponding to format specifiers in fmt

## Dependencies
- Functions called/Symbols referenced:
  - [ErrorData](../E/ErrorData.md) (error data structure)
  - CHECK_STACK_DEPTH (recursion safety check)
  - EVALUATE_MESSAGE (message processing macro)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (memory management)

- Called from (representative examples):
  - Various internal PostgreSQL subsystems when reporting internal errors
  - Low-level storage management functions
  - Authentication and security modules
  - WAL (Write-Ahead Logging) subsystem
  - Index and heap access methods

## Notes and Other Information
- Returns 0 (return value is not meaningful)
- Manages recursion depth to prevent infinite error loops
- Switches memory context during operation for safe memory management
- Part of PostgreSQL's comprehensive error reporting infrastructure
- Should be used sparingly and only for truly internal error conditions
- Not intended for user-facing error messages that should be translatable