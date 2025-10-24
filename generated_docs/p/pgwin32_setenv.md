# pgwin32_setenv

## Location
[src/port/win32env.c:121-149](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/win32env.c#L121-L149)

## Overview
Windows-specific replacement for POSIX setenv() that provides a convenient interface for setting environment variables with optional overwrite control, internally using pgwin32_putenv() for cross-CRT compatibility.

## Definition
```c
int pgwin32_setenv(const char *name, const char *value, int overwrite)
```

## Detailed Description
This function provides a POSIX-compatible interface for setting environment variables on Windows systems. It acts as a wrapper around pgwin32_putenv(), providing the standard three-parameter setenv() signature that separates the variable name and value, along with overwrite control.

The function performs comprehensive input validation following POSIX standards, checking for NULL pointers, empty names, and invalid characters (specifically '=' in the variable name). It respects the overwrite parameter, allowing callers to specify whether existing variables should be replaced or left unchanged.

Internally, it constructs the "NAME=VALUE" string format required by pgwin32_putenv() and leverages that function's comprehensive CRT compatibility mechanism to ensure the environment change is visible across all loaded runtime libraries.

## Parameters / Member Variables
- `name`: The name of the environment variable to set. Must not be NULL, empty, or contain '=' characters.
- `value`: The value to assign to the environment variable. Must not be NULL.
- `overwrite`: Controls behavior when the variable already exists. If 0, existing variables are not modified; if non-zero, existing variables are overwritten.

## Dependencies
- Functions called/Symbols referenced:
  - strchr (C standard library)
  - getenv (C standard library)
  - malloc (C standard library)
  - sprintf (C standard library)
  - [pgwin32_putenv](pgwin32_putenv.md) (internal function)
  - free (C standard library)
- Called from (representative examples):
  - setenv macro replacement (src/include/port/win32_port.h:535)

## Notes and Other Information
- Returns 0 on success, -1 on failure with errno set to EINVAL for invalid parameters
- Follows POSIX setenv() semantics exactly, making it a drop-in replacement for portable code
- The overwrite parameter follows POSIX convention: 0 means don't overwrite, any other value means overwrite
- Memory allocation failure during string construction will cause the function to return -1
- Provides a higher-level interface compared to pgwin32_putenv(), handling string formatting and validation automatically
- Essential for maintaining code portability between Unix and Windows builds of PostgreSQL

## Simplified Source

```c
int pgwin32_setenv(const char *name, const char *value, int overwrite) {
    // Validate input parameters
    if (name == NULL || name[0] == '\0' || strchr(name, '=') != NULL || value == NULL) {
        errno = EINVAL;
        return -1;
    }

    // Don't overwrite if not requested and variable exists
    if (overwrite == 0 && getenv(name) != NULL)
        return 0;

    // Create "name=value" string and set environment variable
    char *envstr = malloc(strlen(name) + strlen(value) + 2);
    if (!envstr)
        return -1;

    sprintf(envstr, "%s=%s", name, value);
    int result = pgwin32_putenv(envstr);
    free(envstr);

    return result;
}
```