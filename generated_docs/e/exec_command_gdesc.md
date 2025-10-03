# exec_command_gdesc

## Location
[src/bin/psql/command.c:1563-1579](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L1563-L1579)

## Overview
Implements the \gdesc command in psql, which enables describe mode to show column information for the next query result instead of the actual data.

## Definition

```c
static backslashResult
exec_command_gdesc(PsqlScanState scan_state, bool active_branch)
```
## Detailed Description
This function handles the \gdesc backslash command in psql. When executed, it sets a flag that causes the next query to return column descriptions (metadata) rather than query results. This is useful for inspecting the structure of query results without actually executing the query or viewing the data. The function is simple, only setting the gdesc_flag when in an active branch.

## Parameters / Member Variables
- `scan_state`: Scanner state for reading command options (unused in this implementation)
- `active_branch`: Whether to actually set the flag (true) or just parse the command (false)
## Dependencies
- Functions called/Symbols referenced:
  - PSQL_CMD_SKIP_LINE
  - PSQL_CMD_SEND
- Called from (representative examples):
  - [exec_command](exec_command.md)

## Notes and Other Information
- Sets pset.gdesc_flag to true when active_branch is true
- Returns PSQL_CMD_SEND to indicate the next query should be processed with describe mode
- No command-line options are processed for this command
- The actual description logic is handled elsewhere in the query processing pipeline