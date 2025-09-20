# exec_command_crosstabview

## Location
[src/bin/psql/command.c:749-772](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L749-L772)

## Overview
Processes the \crosstabview psql command which executes a query and displays the result in a crosstab (pivot table) format.

## Definition

```c
static backslashResult
exec_command_crosstabview(PsqlScanState scan_state, bool active_branch)
```
## Detailed Description
This function handles the \crosstabview command in psql, which allows users to pivot query results into a crosstab format. When the command is active, it parses up to 4 arguments that specify how to organize the crosstab display: vertical column, horizontal column, data column, and sort column. The function sets up the necessary state in the global pset structure to enable crosstab formatting for the next query execution. If the branch is not active (e.g., within a false conditional), it simply ignores the slash options without processing them.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer containing the current parsing state for reading command arguments
- `active_branch`: Boolean indicating whether this command should be executed (true) or ignored due to conditional logic (false)

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option
  - [ignore_slash_options](../i/ignore_slash_options.md)
  - lengthof (macro)
- Called from (representative examples):
  - [exec_command](exec_command.md) (main command dispatcher)

## Notes and Other Information
- The function supports up to 4 arguments stored in pset.ctv_args array
- Sets pset.crosstab_flag to true to enable crosstab processing for the subsequent query
- Returns PSQL_CMD_SEND when active to indicate the query should be sent to the server
- Returns PSQL_CMD_SKIP_LINE as default status
- Part of psql's backslash command system for interactive query formatting