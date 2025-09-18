# pgwin32_putenv

## Location
[src/port/win32env.c:27-120](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/win32env.c#L27-L120)

## Overview
Windows-specific replacement for POSIX putenv() that updates environment variables across all loaded CRT modules and the Windows process environment to ensure consistency across third-party libraries.

## Definition
```c
int pgwin32_putenv(const char *envval)
```

## Detailed Description
This function provides a comprehensive solution for setting environment variables on Windows systems. Unlike the standard POSIX putenv(), it doesn't use the passed-in string as permanent storage and instead makes its own copy. The function addresses a critical Windows-specific issue where different CRT (C Runtime) libraries maintain separate copies of the environment.

The function operates in three phases:
1. Updates the Windows process environment using SetEnvironmentVariable() to make changes visible to child processes and future CRT initializations
2. Iterates through all currently loaded CRT modules (msvcrt, msvcr70-120, ucrtbase, etc.) and calls their respective _putenv() functions to update each CRT's environment copy
3. Updates PostgreSQL's own CRT environment as a fallback

This comprehensive approach ensures that environment variable changes are visible to all third-party libraries regardless of which CRT they link against, solving compatibility issues that would otherwise occur with mixed CRT environments.

## Parameters / Member Variables
- `envval`: A string in the format "NAME=VALUE" specifying the environment variable to set. The function creates its own copy of this string for processing.

## Dependencies
- Functions called/Symbols referenced:
  - strdup (C standard library)
  - strchr (C standard library) 
  - SetEnvironmentVariable (Windows API)
  - GetModuleHandleEx (Windows API)
  - GetProcAddress (Windows API)
  - FreeLibrary (Windows API)
  - _putenv (CRT function)
- Called from (representative examples):
  - [pgwin32_setenv](pgwin32_setenv.md) (src/port/win32env.c:144)
  - [pgwin32_unsetenv](pgwin32_unsetenv.md) (src/port/win32env.c:160)
  - putenv macro replacement (src/include/port/win32_port.h:534)

## Notes and Other Information
- The function supports Visual Studio versions from 6.0 through 2015+ by targeting their respective CRT modules
- Only calls SetEnvironmentVariable() when adding variables, not when removing them, to avoid crashes on certain MinGW versions
- Returns -1 on failure (memory allocation or invalid format), 0 on success
- Critical for maintaining environment variable consistency in Windows builds where multiple CRTs may be loaded simultaneously
- The comprehensive CRT module list ensures compatibility across different development environments and third-party libraries