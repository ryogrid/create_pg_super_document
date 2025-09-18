# errdetail

## Location
src/backend/utils/error/elog.c: 1203 - 1229

## Overview
Adds a detail error message text to the current error, providing additional context and information to complement the primary error message.

## Definition
```c
int errdetail(const char *fmt, ...) pg_attribute_printf(1, 2);
```

## Detailed Description
`errdetail` provides additional detail information for error messages in PostgreSQL's error reporting system. This function is typically called after setting a primary error message with `errmsg()` or similar functions to provide more specific context about what went wrong.

The detail message appears as a separate field in error reports and can be formatted independently from the primary message. This allows for hierarchical error information where the primary message gives the general nature of the problem and the detail provides specific diagnostic information.

Like other error reporting functions, `errdetail` operates within PostgreSQL's error handling framework, managing memory context switching and recursion depth for safe operation during error conditions. The detail message is subject to internationalization and will be translated according to the current locale.

## Parameters / Member Variables
- `fmt`: Format string for the detail message (printf-style)
- `...`: Variable arguments corresponding to format specifiers in fmt

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (error data structure)
  - CHECK_STACK_DEPTH (recursion safety check)
  - EVALUATE_MESSAGE (message processing macro with translation enabled)
  - MemoryContextSwitchTo (memory management)

- Called from (representative examples):
  - PL/pgSQL execution functions (providing SQL statement context)
  - PL/Python type conversion functions (providing type mismatch details)
  - JSON parsing functions (providing parse error context)
  - Common utility functions (providing operation-specific details)
  - Test modules (providing test failure specifics)

## Notes and Other Information
- Returns 0 (return value is not meaningful)
- Manages recursion depth to prevent infinite error loops
- Switches memory context during operation for safe memory management
- Messages are translatable and subject to internationalization
- Typically used in conjunction with errmsg() for complete error reporting
- Detail messages should provide specific diagnostic information, not repeat the primary message
- Part of PostgreSQL's structured error reporting that allows clients to display errors in organized fashion
- Detail information can help users understand and resolve the underlying problem