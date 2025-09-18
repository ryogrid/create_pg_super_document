# win32_read_locale

## Location
src/backend/commands/collationcmds.c: 778 - 839

## Overview
A Windows callback function used by EnumSystemLocalesEx() to create PostgreSQL collations for each available system locale, including POSIX-style aliases.

## Definition
```c
static BOOL CALLBACK win32_read_locale(LPWSTR pStr, DWORD dwFlags, LPARAM lparam)
```

## Detailed Description
This function serves as a callback for the Windows API function EnumSystemLocalesEx(), which enumerates all available system locales on Windows. For each locale, it converts the wide-character locale name to a multibyte string, validates it, and attempts to create a PostgreSQL collation using create_collation_from_locale(). Additionally, it creates POSIX-style aliases by converting Windows-style hyphens to underscores (e.g., "en-US" becomes "en_US"). The function always returns TRUE to continue enumeration, handling errors gracefully by skipping invalid or problematic locales. This is a Windows-specific implementation used only when ENUM_SYSTEM_LOCALE is defined.

## Parameters / Member Variables
- `pStr`: Wide-character string containing the locale name from Windows
- `dwFlags`: Flags from EnumSystemLocalesEx (unused, marked with (void) cast)
- `lparam`: User-defined parameter cast to CollParam* containing namespace ID and counters

## Dependencies
- Functions called/Symbols referenced:
  - WideCharToMultiByte (Windows API)
  - GetLastError (Windows API)
  - create_collation_from_locale
  - strchr (C library)
  - strcpy (C library)
  - [CollationCreate](../C/CollationCreate.md)
  - [GetUserId](../G/GetUserId.md)
  - [get_collation_actual_version](../g/get_collation_actual_version.md)
  - CommandCounterIncrement
  - CollParam (struct type)
  - NAMEDATALEN (constant)
- Called from (representative examples):
  - [pg_import_system_collations](../p/pg_import_system_collations.md) (via EnumSystemLocalesEx callback)

## Notes and Other Information
- This is a Windows-specific function, only compiled when ENUM_SYSTEM_LOCALE is defined
- Uses the CALLBACK calling convention required by Windows API callback functions
- Converts Windows locale names (with hyphens) to POSIX-style names (with underscores) for compatibility
- Handles buffer overflow gracefully by skipping locales with names too long for NAMEDATALEN
- Always returns TRUE to continue enumeration, never stopping the process early
- Part of PostgreSQL's platform-specific collation import system for Windows
- Creates both the original Windows-style collation and a POSIX-style alias when hyphens are present