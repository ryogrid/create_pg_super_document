# process_command_g_options

## Location
[src/bin/psql/command.c:1488-1562](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L1488-L1562)

## Overview
Processes parenthesized pset options for the \g command in psql, parsing and applying display formatting options specified within parentheses.

## Definition

```c
static backslashResult
process_command_g_options(char *first_option, PsqlScanState scan_state,
						  bool active_branch, const char *cmd)
```
## Detailed Description
This function handles the parsing and application of pset (print settings) options enclosed in parentheses that follow \g commands in psql. It iterates through options separated by spaces or commas, supporting both "name" and "name=value" formats. The function temporarily modifies print settings and can restore them if parsing fails. Options are applied only when active_branch is true, allowing for conditional execution in psql scripts.

## Parameters / Member Variables
- : The first option string (may be modified but not freed by this function)
- : Scanner state for reading additional options from input stream
- : Whether to actually apply the options (true) or just parse them (false)
- : The command name for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option
  - [savePsetInfo](../s/savePsetInfo.md)
  - do_pset
  - [restorePsetInfo](../r/restorePsetInfo.md)
  - pg_log_error
- Called from (representative examples):
  - [exec_command_g](../e/exec_command_g.md)

## Notes and Other Information
- Saves current pset state before applying options to enable rollback on failure
- Options can be restored if parsing fails after some options have been applied
- The function handles memory management carefully, never freeing the first_option parameter
- Returns PSQL_CMD_SKIP_LINE on success or PSQL_CMD_ERROR on failure