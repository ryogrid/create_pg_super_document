# show_unix_socket_permissions

## Location
[src/backend/commands/variable.c:1180-1194](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/variable.c#L1180-L1194)

## Overview
A GUC (Grand Unified Configuration) show hook function that formats and returns the current value of the unix_socket_permissions configuration parameter as an octal string.

## Definition

```c
const char *
show_unix_socket_permissions(void)
```
## Detailed Description
This function serves as a show hook for the unix_socket_permissions GUC parameter in PostgreSQL. Show hooks are callback functions used by PostgreSQL's configuration system to format parameter values for display when queried by users (e.g., via SHOW commands or by examining pg_settings). The function converts the internal integer representation of Unix socket permissions into a human-readable 4-digit octal format (e.g., "0700", "0755").

The function uses a static buffer to store the formatted string, which is safe because GUC show hooks are called in controlled contexts where the returned string is immediately used or copied.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - snprintf (standard library function)
  - Unix_socket_permissions (global variable)
- Called from (representative examples):
  - Referenced in GUC_HOOKS_H header file for GUC system integration

## Notes and Other Information
- Uses a static buffer of 12 characters to store the octal representation
- The %04o format specifier ensures 4-digit octal output with leading zeros
- This is part of PostgreSQL's GUC (Grand Unified Configuration) system infrastructure
- The unix_socket_permissions parameter controls the file permissions for Unix domain sockets created by PostgreSQL