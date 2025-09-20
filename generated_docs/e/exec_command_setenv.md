# exec_command_setenv

## Location
[src/bin/psql/command.c:2474-2521](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L2474-L2521)

## Overview
Implements the psql \setenv backslash command that sets or unsets operating system environment variables.

## Definition

```c
enumbers = (strchr(cmd, '+') != NULL);
```
## Detailed Description
The  function handles the execution of the \setenv backslash command in psql, which manages operating system environment variables. The command operates in two modes: when provided with both a variable name and value, it sets the environment variable; when provided with only a variable name (no value), it unsets (removes) the environment variable from the process environment.

The function includes validation to ensure the environment variable name is valid - specifically, it checks that the variable name does not contain an equals sign ('='), which would be invalid for environment variable names. The function uses the standard C library functions  and  to manipulate the process environment, with the third parameter to  set to 1 to allow overwriting existing variables.

## Parameters / Member Variables
- : Scanner state object used to parse the environment variable name and optional value from the command line
- : Boolean flag indicating whether this command should actually execute (used for conditional execution in psql)
- : The name of the command being executed (used for error reporting in log messages)

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option
  - pg_log_error
  - strchr
  - setenv
  - unsetenv
  - [ignore_slash_options](../i/ignore_slash_options.md)
  - free
  - [PsqlScanState](../P/PsqlScanState.md) (type)
  - [backslashResult](../b/backslashResult.md) (return type)
  - OT_NORMAL (option type)
  - PSQL_CMD_SKIP_LINE (success return value)
  - PSQL_CMD_ERROR (error return value)
- Called from (representative examples):
  - [exec_command](exec_command.md)
  - EditableObjectType (indirectly through command dispatch)

## Notes and Other Information
- This function provides interface to operating system environment variables, distinct from psql's internal variables managed by \set
- Environment variables set with this command affect the psql process and any subprocesses it spawns
- The function validates that environment variable names do not contain '=' characters, which is a requirement for valid environment variable names
- When only a variable name is provided (no value), the variable is removed from the environment using 
- The  call uses overwrite flag set to 1, meaning existing environment variables with the same name will be replaced
- Proper memory management includes cleanup of both the variable name and value strings
- The function properly handles conditional execution by ignoring options when not in an active branch
- Returns  if the variable name is missing or contains invalid characters, otherwise returns 
- Changes made to environment variables persist for the duration of the psql session and affect any external commands executed from within psql