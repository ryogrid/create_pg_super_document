# log_error

## Location
[src/port/win32security.c:20-48](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/win32security.c#L20-L48)

## Overview
A preprocessor macro that provides a convenient interface for logging error messages using PostgreSQL's internal error reporting system.

## Definition

```c
#endif

static void log_error(const char *fmt,...) pg_attribute_printf(1, 2);


/*
 * Utility wrapper for frontend and backend when reporting an error
 * message.
 */
static void
log_error(const char *fmt,...)
```
## Detailed Description
The  macro is a utility wrapper around PostgreSQL's  function, specifically designed for logging error messages at the LOG level. It simplifies the process of reporting internal errors by automatically handling the message formatting and error level specification. The macro uses variadic arguments to accept flexible message formatting, similar to printf-style functions, and internally calls  to format the message appropriately for internal system use.

This macro is primarily used within PostgreSQL's common utility functions, particularly in , where it handles various system-level errors related to process execution, file operations, and Windows-specific security operations.

## Parameters / Member Variables
- : Error code function that specifies the type of error being reported
- : Variadic arguments for the error message format string and its parameters (printf-style formatting)

## Dependencies
- Functions called/Symbols referenced:
  - ereport
  - [errmsg_internal](../e/errmsg_internal.md)
  - LOG (error level constant)
- Called from (representative examples):
  - [find_my_exec](../f/find_my_exec.md)
  - [normalize_exec_path](../n/normalize_exec_path.md)
  - [pipe_read_line](../p/pipe_read_line.md)
  - [pclose_check](../p/pclose_check.md)
  - [AddUserToTokenDacl](../A/AddUserToTokenDacl.md)
  - [GetTokenUser](../G/GetTokenUser.md)
  - [pgwin32_is_admin](../p/pgwin32_is_admin.md)

## Notes and Other Information
- This macro is defined in  and is used extensively throughout PostgreSQL's common utility functions
- The LOG level indicates informational messages that should be logged but are not necessarily errors from the user's perspective
- The use of  suggests this macro is intended for internal system messages rather than user-facing error messages
- Heavily used in Windows-specific code paths for security and process management operations
- The macro provides a clean abstraction that maintains consistency in error reporting across the codebase