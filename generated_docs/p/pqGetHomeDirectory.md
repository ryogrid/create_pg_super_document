# pqGetHomeDirectory

## Location
src/interfaces/libpq/fe-connect.c: 7667 - 7693

## Overview
Obtains the user's home directory path in a cross-platform manner for PostgreSQL client library usage.

## Definition
```c
bool pqGetHomeDirectory(char *buf, int bufsize)
```

## Detailed Description
This function retrieves the user's home directory and stores it in the provided buffer. On Unix systems, it returns the actual user home directory by first checking the HOME environment variable, and if that fails, using pg_get_user_home_dir() with the effective user ID. On Windows, it returns the PostgreSQL-specific application data folder (APPDATA/postgresql). This is a libpq-specific implementation that avoids pulling in path.c to prevent namespace pollution in applications using libpq.

## Parameters / Member Variables
- `buf`: Character buffer to store the home directory path
- `bufsize`: Size of the buffer in bytes

## Dependencies
- Functions called/Symbols referenced:
  - getenv (Unix: get HOME environment variable)
  - geteuid (Unix: get effective user ID)
  - pg_get_user_home_dir (Unix: fallback home directory lookup)
  - strlcpy (Unix: safe string copy)
  - SHGetFolderPath (Windows: get application data folder)
  - snprintf (Windows: format path string)
- Called from (representative examples):
  - pqConnectOptions2
  - [parseServiceInfo](parseServiceInfo.md)
  - [initialize_SSL](../i/initialize_SSL.md)

## Notes and Other Information
- Returns true on success, false on failure
- Failure should generally not be treated as an error - applications should handle gracefully
- Unix implementation: checks HOME environment variable first, then falls back to user database lookup
- Windows implementation: uses CSIDL_APPDATA + "/postgresql" subdirectory
- Designed to avoid namespace pollution by not using get_home_path() from path.c
- Some users intentionally run in home-directory-less environments
- Buffer must be large enough to hold the resulting path