# exec_command_help

## Location
[src/bin/psql/command.c:1663-1682](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L1663-L1682)

## Overview
Implements the \help command in PostgreSQL's psql client, providing SQL command help to users.

## Definition

```c
static backslashResult
exec_command_help(PsqlScanState scan_state, bool active_branch)
```
## Detailed Description
This function handles the execution of the \help backslash command in psql. When invoked, it extracts the optional topic parameter from the command line and calls the helpSQL function to display help information about SQL commands. The function respects the active_branch parameter for conditional execution in psql scripts (used with \if constructs). If the command is not in an active branch, it simply ignores the remainder of the line without processing.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer that tracks the current parsing state of the psql input
- `active_branch`: Boolean flag indicating whether this command is being executed in an active conditional branch
## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option (extracts command arguments from input)
  - helpSQL (displays SQL command help)
  - [ignore_slash_whole_line](../i/ignore_slash_whole_line.md) (skips command processing when in inactive branch)
  - free (deallocates memory for the option string)
- Called from (representative examples):
  - [exec_command](exec_command.md) (main command dispatcher in psql)

## Notes and Other Information
- Returns PSQL_CMD_SKIP_LINE to indicate the entire line has been processed
- Uses OT_WHOLE_LINE option type to capture the entire remainder of the line as the help topic
- Integrates with psql's pager settings (pset.popt.topt.pager) for output formatting
- Part of psql's backslash command infrastructure for interactive SQL operations

## Simplified Source

```c
static backslashResult exec_command_help(PsqlScanState scan_state, bool active_branch) {
    if (active_branch) {
        // Extract help topic (entire remainder of line)
        char *opt = psql_scan_slash_option(scan_state, OT_WHOLE_LINE, NULL, true);

        // Display SQL help for the topic
        helpSQL(opt, pset.popt.topt.pager);
        free(opt);
    } else {
        // Not in active branch - skip entire line
        ignore_slash_whole_line(scan_state);
    }

    return PSQL_CMD_SKIP_LINE;
}
```