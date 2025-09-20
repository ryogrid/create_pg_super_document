# exec_command_f

## Location
[src/bin/psql/command.c:1407-1434](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L1407-L1434)

## Overview
Implements the \f command in psql, which sets the field separator used in output formatting.

## Definition

```c
static backslashResult
exec_command_f(PsqlScanState scan_state, bool active_branch)
```
## Detailed Description
This function handles the \f command which changes the field separator character or string used when formatting query output. The command accepts an optional argument specifying the new field separator. The actual field separator setting is handled by the do_pset() function with the "fieldsep" parameter. The function returns different values based on whether the operation succeeded or failed.

## Parameters / Member Variables
- : Scanner state for parsing the command line arguments
- : Boolean indicating whether this command should be executed (used for conditional processing)

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option
  - do_pset
  - [ignore_slash_options](../i/ignore_slash_options.md)
  - free
  - [PsqlScanState](../P/PsqlScanState.md) (type)
  - OT_NORMAL (constant)
  - PSQL_CMD_SKIP_LINE (return value for success)
  - PSQL_CMD_ERROR (return value for failure)
  - [backslashResult](../b/backslashResult.md) (return type)
- Called from (representative examples):
  - [exec_command](exec_command.md)
  - EditableObjectType (indirectly through command dispatch)

## Notes and Other Information
- Unlike many other command functions, this one can return either PSQL_CMD_SKIP_LINE or PSQL_CMD_ERROR based on success
- The field separator setting is stored in pset.popt and affects subsequent query output formatting
- Memory allocated for the field separator argument is properly freed after use
- When active_branch is false, arguments are consumed but not processed
- The do_pset() function handles the actual validation and setting of the field separator value
- The pset.quiet flag is passed to do_pset() to control whether status messages are displayed