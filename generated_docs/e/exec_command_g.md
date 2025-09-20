# exec_command_g

## Location
[src/bin/psql/command.c:1435-1487](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L1435-L1487)

## Overview
Implements the \g and \gx commands in psql, which send the current query buffer for execution with optional output redirection and formatting options.

## Definition

```c
static backslashResult
exec_command_g(PsqlScanState scan_state, bool active_branch, const char *cmd)
```
## Detailed Description
This function handles both \g and \gx commands which execute the current query buffer. The commands support:
- Optional pset formatting options specified in parentheses
- Optional filename or shell command for output redirection  
- \gx automatically enables expanded output format in addition to any other specified options

The function processes arguments in two phases: first parsing any pset options enclosed in parentheses, then handling the output destination. It uses different return values to indicate whether the query should be sent (PSQL_CMD_SEND) or if there was an error.

## Parameters / Member Variables
- : Scanner state for parsing the command line arguments
- : Boolean indicating whether this command should be executed (used for conditional processing)  
- : String identifying the specific command ("g" or "gx")

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option
  - [process_command_g_options](../p/process_command_g_options.md)
  - [expand_tilde](expand_tilde.md)
  - [pg_strdup](../p/pg_strdup.md)
  - [savePsetInfo](../s/savePsetInfo.md)
  - strcmp
  - free
  - [PsqlScanState](../P/PsqlScanState.md) (type)
  - OT_FILEPIPE (constant)
  - PSQL_CMD_SKIP_LINE (return value)
  - PSQL_CMD_SEND (return value)
  - [backslashResult](../b/backslashResult.md) (return type)
- Called from (representative examples):
  - [exec_command](exec_command.md)
  - EditableObjectType (indirectly through command dispatch)

## Notes and Other Information
- Complex argument processing occurs regardless of active_branch status to maintain parser consistency
- The \gx variant automatically saves current pset options and forces expanded output mode
- Output filename undergoes tilde expansion for home directory references
- Memory management includes proper freeing of allocated filename strings
- The function can return PSQL_CMD_SEND to trigger query execution, unlike most command functions
- Pset options are temporarily applied only for the duration of this query execution
- The gsavepopt mechanism allows restoring previous formatting settings after the query