# pg_strerror_r

## Location
[src/port/strerror.c:46-84](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/strerror.c#L46-L84)

## Overview
A thread-safe, slightly cleaned-up version of strerror_r() that provides robust error message strings with platform-specific handling and fallback mechanisms.

## Definition
```c
char *pg_strerror_r(int errnum, char *buf, size_t buflen)
```

## Detailed Description
This function serves as PostgreSQL's robust, thread-safe interface for converting error numbers to human-readable error messages. It handles multiple scenarios including Windows Winsock errors, platform-specific strerror_r() implementations, and provides intelligent fallbacks when standard functions fail. The function attempts to get a meaningful error message through multiple methods: first trying platform-specific socket error handling (on Windows), then the platform's strerror_r(), and finally falling back to symbolic errno names or numeric representations.

## Parameters / Member Variables
- `errnum`: The error number (errno value) to convert to a descriptive string
- `buf`: Buffer to store the error message string
- `buflen`: Size of the provided buffer

## Dependencies
- Functions called/Symbols referenced:
  - [win32_socket_strerror](../w/win32_socket_strerror.md) (Windows socket error handling)
  - [gnuish_strerror_r](../g/gnuish_strerror_r.md) (platform-specific strerror_r wrapper)
  - [get_errno_symbol](../g/get_errno_symbol.md) (symbolic errno name lookup)
- Called from (representative examples):
  - [pg_strerror](pg_strerror.md)

## Notes and Other Information
- Thread-safe due to user-provided buffer management
- Handles Windows Winsock errors (error codes 10000-11999) specially on WIN32 platforms
- Provides robust fallback mechanisms when standard error functions return empty, null, or garbled strings
- Falls back to symbolic errno names via get_errno_symbol() when standard functions fail
- Ultimate fallback generates "operating system error [number]" message
- Uses internationalization support with _() macro for the fallback message
- Located in src/port/strerror.c:46-84

## Simplified Source

```c
char *pg_strerror_r(int errnum, char *buf, size_t buflen)
{
    char *str;

    // Handle Windows socket errors specially
#ifdef WIN32
    if (errnum >= 10000 && errnum <= 11999)
        return win32_socket_strerror(errnum, buf, buflen);
#endif

    // Try platform's strerror_r() first
    str = gnuish_strerror_r(errnum, buf, buflen);

    // If we get empty/invalid result, try symbolic errno name
    if (str == NULL || *str == '\0' || *str == '?')
        str = get_errno_symbol(errnum);

    // Final fallback: numeric error message
    if (str == NULL) {
        snprintf(buf, buflen, _("operating system error %d"), errnum);
        str = buf;
    }

    return str;
}
```