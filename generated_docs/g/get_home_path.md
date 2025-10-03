# get_home_path

## Location
[src/port/path.c:1004-1052](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/path.c#L1004-L1052)

## Overview
Retrieves the home path directory for the current user, with platform-specific behavior for Unix/Linux and Windows systems.

## Definition

```c
bool
get_home_path(char *ret_path)
```
## Detailed Description
This function returns the user's home directory path on Unix/Linux systems, or the PostgreSQL-specific application data folder on Windows. On Unix systems, it first checks the HOME environment variable, and if that's unset or empty, it falls back to retrieving the home directory information from the password database using . On Windows, it uses the APPDATA environment variable and appends '/postgresql' to create a PostgreSQL-specific directory path.

The function is designed to provide a consistent interface for obtaining a user-specific directory path across different platforms, which is essential for storing user configuration files and application data.

## Parameters / Member Variables
- `*ret_path`: Output buffer to store the retrieved home path (must be at least MAXPGPATH bytes)
## Dependencies
- Functions called/Symbols referenced:
  - [pg_get_user_home_dir](../p/pg_get_user_home_dir.md)
  - [strlcpy](../s/strlcpy.md)
- Called from (representative examples):
  - [expand_tilde](../e/expand_tilde.md)
  - [initializeInput](../i/initializeInput.md)
  - [process_psqlrc](../p/process_psqlrc.md)

## Notes and Other Information
- Returns true on success, false on failure
- On Unix systems, prioritizes the HOME environment variable over system user database
- On Windows, creates a PostgreSQL-specific subdirectory in the application data folder
- The function uses  on Windows instead of more modern APIs to avoid linking dependencies that would consume desktop heap memory
- The ret_path buffer must be at least MAXPGPATH bytes in size to accommodate the full path