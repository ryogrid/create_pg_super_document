# errdetail_log

## Location
src/backend/utils/error/elog.c: 1251 - 1271

## Overview
Adds a detail_log error message text to the current error, providing detailed information that will appear only in server logs and not be sent to clients.

## Definition
```c
int errdetail_log(const char *fmt, ...) pg_attribute_printf(1, 2);
```

## Detailed Description
`errdetail_log` provides detailed error information that is specifically designed to appear only in server logs and not be transmitted to client applications. This allows PostgreSQL to log comprehensive diagnostic information for server administrators and developers while keeping client-facing error messages appropriately sanitized.

The function is part of PostgreSQL's dual-level error reporting system where different levels of detail can be provided for different audiences. The detail_log information typically contains sensitive or highly technical information that would be inappropriate or confusing to send to client applications but is valuable for server-side troubleshooting and debugging.

Like other error reporting functions, it operates within PostgreSQL's error handling framework, managing memory context switching and recursion depth for safe operation during error conditions. The detail_log message is subject to internationalization and will be translated according to the current locale.

## Parameters / Member Variables
- `fmt`: Format string for the detail log message (printf-style)
- `...`: Variable arguments corresponding to format specifiers in fmt

## Dependencies
- Functions called/Symbols referenced:
  - ErrorData (error data structure)
  - CHECK_STACK_DEPTH (recursion safety check)
  - EVALUATE_MESSAGE (message processing macro with translation enabled)
  - MemoryContextSwitchTo (memory management)

- Called from (representative examples):
  - Dependency tracking functions (providing object dependency details)
  - Authentication functions (providing detailed authentication failure information)
  - Tablespace operations (providing detailed filesystem information)
  - User management (providing detailed role operation information)
  - Deadlock detector (providing comprehensive lock information)
  - Process sleep functions (providing detailed wait information)

## Notes and Other Information
- Returns 0 (return value is not meaningful)
- Manages recursion depth to prevent infinite error loops
- Switches memory context during operation for safe memory management
- Messages are translatable and subject to internationalization
- Detail_log messages appear ONLY in server logs, never sent to clients
- Provides security by preventing sensitive information from reaching clients
- Commonly used for authentication failures, security-related events, and detailed system diagnostics
- Part of PostgreSQL's structured error reporting with security-conscious message routing
- Enables comprehensive server-side logging while maintaining appropriate client information boundaries
- Essential for debugging and monitoring in production environments where client access to detailed internal state would be inappropriate