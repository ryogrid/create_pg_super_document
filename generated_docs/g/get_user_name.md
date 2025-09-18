# get_user_name

## Location
[src/common/username.c:31-73](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/username.c#L31-L73)

## Overview
Returns the current user name as a string, handling platform-specific differences between Unix-like systems and Windows.

## Definition
```c
const char *get_user_name(char **errstr)
```

## Detailed Description
This function retrieves the current effective user name using platform-specific system calls. On Unix-like systems, it uses `geteuid()` to get the effective user ID and then `getpwuid()` to resolve it to a username. On Windows, it uses `GetUserName()` from the Windows API. The function provides error handling by setting an error string pointer when operations fail, allowing callers to handle errors gracefully without the program terminating.

The function returns a pointer to either a system-managed buffer (Unix) or a static buffer (Windows), so the returned string should not be modified or freed by the caller.

## Parameters / Member Variables
- `errstr`: A pointer to a char pointer that will be set to point to an allocated error message string if the function fails, or NULL on success

## Dependencies
- Functions called/Symbols referenced:
  - `geteuid` (Unix systems)
  - `getpwuid` (Unix systems) 
  - `GetUserName` (Windows systems)
  - [psprintf](../p/psprintf.md)
  - `strerror`
  - `GetLastError` (Windows systems)
- Called from (representative examples):
  - [get_user_info](get_user_info.md) (src/bin/pg_upgrade/util.c:335)
  - [get_user_name_or_exit](get_user_name_or_exit.md) (src/common/username.c:79)
  - [config_sspi_auth](../c/config_sspi_auth.md) (src/test/regress/pg_regress.c:1024)

## Notes and Other Information
- The function handles platform differences transparently - Unix systems use POSIX user ID resolution while Windows uses native API calls
- On Unix systems, the returned pointer points to system-managed memory that should not be freed
- On Windows systems, the returned pointer points to a static buffer with a maximum size of 257 characters (UNLEN+1)
- Error handling follows PostgreSQL conventions by setting an error string that can be displayed to users
- The function sets errno to 0 before system calls to ensure clean error detection