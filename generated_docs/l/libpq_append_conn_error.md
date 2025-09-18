# libpq_append_conn_error

## Location
src/interfaces/libpq/fe-misc.c: 1380 - 1401

## Overview
A utility function that appends a formatted, translated error message to a PostgreSQL connection's error message buffer with automatic newline termination.

## Definition


## Detailed Description
This function provides a convenient way to append formatted error messages to a PostgreSQL connection's error buffer. It handles several important aspects of error message management:

1. **Translation**: The format string is automatically translated using  for internationalization support
2. **Formatting**: Supports variable arguments (va_list) for printf-style formatting 
3. **Automatic newline**: Appends a newline character to the end of the message automatically
4. **Buffer management**: Handles buffer expansion if needed through a retry loop
5. **Error state preservation**: Preserves the original errno value throughout the operation
6. **Safety checks**: Validates that the format string doesn't already end with a newline and checks for broken buffer state

The function uses a retry loop to handle cases where the error message buffer needs to be enlarged to accommodate the new message.

## Parameters / Member Variables
- : Pointer to the PostgreSQL connection object (PGconn) whose error message buffer will be updated
- : Format string for the error message (should NOT end with a newline as one is automatically appended)
- : Variable arguments corresponding to format specifiers in the format string

## Dependencies
- Functions called/Symbols referenced:
  - : Checks if the connection's error message buffer is in a broken state
  - : Translates the format string for internationalization
  - : Appends the formatted message to the buffer using variable arguments
  - : Appends the trailing newline character to the buffer
- Called from (representative examples):
  - No direct callers found in the current codebase analysis

## Notes and Other Information
- The function includes an assertion to ensure the format string doesn't end with a newline, as this would result in double newlines
- Error handling is robust - if the buffer is already in a broken state, the function returns early without attempting to append
- The retry loop ensures that buffer expansion is handled transparently if the initial buffer space is insufficient
- This is an internal libpq utility function primarily used for consistent error message formatting across the library
- Located in  at lines 1380-1401