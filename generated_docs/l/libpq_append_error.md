# libpq_append_error

## Location
[src/interfaces/libpq/fe-misc.c:1351-1379](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-misc.c#L1351-L1379)

## Overview
Appends a formatted, translated error message to a PQExpBuffer with automatic newline termination and proper error handling.

## Definition
void libpq_append_error(PQExpBuffer errorMessage, const char *fmt, ...)

## Detailed Description
libpq_append_error is a comprehensive error message formatting function that combines translation, printf-style formatting, and buffer management. It takes a format string and variable arguments, translates the format string using libpq_gettext(), formats the message with the provided arguments, and appends it to the specified PQExpBuffer. The function automatically adds a newline at the end and includes robust error handling for buffer operations.

The function preserves errno across its operation and includes retry logic to handle buffer enlargement if needed. It also validates that the format string doesn't end with a newline (since one is automatically added) and gracefully handles cases where the buffer is already in a broken state.

## Parameters / Member Variables
- : The PQExpBuffer to append the error message to
- : The format string for the error message (should not end with newline)
- : Variable arguments for printf-style formatting

## Dependencies
- Functions called/Symbols referenced:
  - PQExpBufferBroken
  - [libpq_gettext](libpq_gettext.md)
  - [appendPQExpBufferVA](../a/appendPQExpBufferVA.md)
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
  - strlen (standard C library function)
  - va_start/va_end (standard C library macros)
- Called from (representative examples):
  - [read_attr_value](../r/read_attr_value.md)
  - [pg_fe_getusername](../p/pg_fe_getusername.md)
  - [ldapServiceLookup](ldapServiceLookup.md)
  - [parseServiceInfo](../p/parseServiceInfo.md)
  - [conninfo_init](../c/conninfo_init.md)
  - [conninfo_parse](../c/conninfo_parse.md)

## Notes and Other Information
- Automatically translates format strings using libpq_gettext() for internationalization
- Preserves errno value across the operation to avoid interfering with error handling
- Includes retry logic to handle buffer expansion when needed
- Enforces that format strings don't end with newline (assertion check)
- Gracefully handles broken buffer states by returning early
- Widely used throughout libpq for consistent error message formatting and reporting
- Essential component of libpq's error handling and user feedback system

## Simplified Source
```c
void libpq_append_error(PQExpBuffer errorMessage, const char *fmt, ...) {
    int save_errno = errno;
    bool done;
    va_list args;

    Assert(fmt[strlen(fmt) - 1] != '\n');

    if (PQExpBufferBroken(errorMessage))
        return; // Buffer already failed

    // Format and append the translated message
    do {
        errno = save_errno;
        va_start(args, fmt);
        done = appendPQExpBufferVA(errorMessage, libpq_gettext(fmt), args);
        va_end(args);
    } while (!done); // Retry if buffer needs enlarging

    // Automatically add newline
    appendPQExpBufferChar(errorMessage, '\n');
}
```