# pgwin32_unsetenv

## Location
src/port/win32env.c: 150 - 163

## Overview
Windows-specific replacement for POSIX unsetenv() that removes environment variables by setting them to an empty value, ensuring cross-CRT compatibility through pgwin32_putenv().

## Definition
```c
int pgwin32_unsetenv(const char *name)
```

## Detailed Description
This function provides a POSIX-compatible interface for removing environment variables on Windows systems. It implements the standard unsetenv() behavior by constructing a "NAME=" string (name with empty value) and passing it to pgwin32_putenv() for processing.

The function follows the Windows convention for environment variable removal, where setting a variable to an empty string effectively removes it from the environment. This approach leverages the comprehensive CRT compatibility mechanism provided by pgwin32_putenv() to ensure the variable removal is visible across all loaded runtime libraries.

The implementation is straightforward but essential for maintaining code portability, as it provides the single-parameter unsetenv() interface expected by POSIX-compliant code while handling the Windows-specific requirements internally.

## Parameters / Member Variables
- `name`: The name of the environment variable to remove. Must be a valid variable name string.

## Dependencies
- Functions called/Symbols referenced:
  - malloc (C standard library)
  - sprintf (C standard library)
  - pgwin32_putenv (internal function)
  - free (C standard library)
- Called from (representative examples):
  - unsetenv macro replacement (src/include/port/win32_port.h:536)

## Notes and Other Information
- Returns 0 on success, -1 on failure (typically due to memory allocation failure)
- Follows POSIX unsetenv() semantics, making it a drop-in replacement for portable code
- Uses the Windows convention of setting variables to empty strings to remove them
- Memory allocation is minimal (variable name length + 2 characters for '=' and null terminator)
- Does not perform input validation on the variable name, relying on pgwin32_putenv() for error handling
- Essential component of the Windows environment variable management trio alongside pgwin32_putenv() and pgwin32_setenv()
- Ensures that environment variable removal is consistently applied across all CRT modules loaded in the process