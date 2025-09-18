# errdetail_internal

## Location
[src/backend/utils/error/elog.c:1230-1250](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/elog.c#L1230-L1250)

## Overview
Adds a detail error message text to the current error without translation, intended for technical details that are not worth translating for general users.

## Definition
```c
int errdetail_internal(const char *fmt, ...) pg_attribute_printf(1, 2);
```

## Detailed Description
`errdetail_internal` is exactly like `errdetail()` except that strings passed to it are not translated and are customarily left out of the internationalization message dictionary. This function is designed for detail messages that are deemed not worth translating, typically because they contain technical information that would not be useful to average users.

The function provides additional diagnostic information for errors while bypassing the translation system. This is appropriate for detailed technical information such as internal state descriptions, debugging information, system call details, or other implementation-specific context that would primarily be useful to developers or system administrators rather than end users.

Like other error reporting functions, it operates within PostgreSQL's error handling framework, managing memory context switching and recursion depth for safe operation during error conditions.

## Parameters / Member Variables
- `fmt`: Format string for the detail message (printf-style, not translated)
- `...`: Variable arguments corresponding to format specifiers in fmt

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (error data structure)
  - CHECK_STACK_DEPTH (recursion safety check)
  - EVALUATE_MESSAGE (message processing macro with translation disabled)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (memory management)

- Called from (representative examples):
  - BRIN index functions (providing technical index operation details)
  - WAL logging functions (providing WAL record assembly details)
  - Catalog functions (providing object creation constraint details)
  - Deadlock detector (providing lock graph information)
  - Serializable isolation (providing conflict details)
  - GUC validation (providing parameter validation specifics)
  - Authentication modules (providing technical authentication failure details)

## Notes and Other Information
- Returns 0 (return value is not meaningful)
- Manages recursion depth to prevent infinite error loops
- Switches memory context during operation for safe memory management
- Messages are NOT translated and remain in English
- Should be used for technical details not intended for general user consumption
- Commonly used for providing developer-oriented diagnostic information
- Part of PostgreSQL's structured error reporting system
- Helps separate user-facing error information from technical diagnostic details
- Useful for providing context that aids in debugging without cluttering translated message catalogs