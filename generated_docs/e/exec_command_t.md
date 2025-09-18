# exec_command_t

## Location
src/bin/psql/command.c: 2605 - 2626

## Overview
Implements the `\t` psql command that toggles the display of table headers and row count in query output.

## Definition
```c
static backslashResult
exec_command_t(PsqlScanState scan_state, bool active_branch)
```

## Detailed Description
This function handles the `\t` psql meta-command which controls the "tuples_only" display option. When enabled, this option suppresses the display of column headers and row count information in query results, showing only the actual data rows. The function accepts an optional parameter to explicitly set the mode (on/off), or toggles the current setting if no parameter is provided.

The function delegates the actual setting management to the `do_pset` function, which handles the parsing and validation of the option value and updates the appropriate print option in the global pset structure.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer for parsing command line options
- `active_branch`: Boolean indicating if the command should be executed or skipped

## Dependencies
- Functions called/Symbols referenced:
  - `psql_scan_slash_option` - Parse optional parameter from command line
  - `do_pset` - Set the tuples_only print option
  - [ignore_slash_options](../i/ignore_slash_options.md) - Skip option parsing when not in active branch
- Called from:
  - [exec_command](exec_command.md) - Main command dispatcher for `\t` command

## Notes and Other Information
- Controls the "tuples_only" print option which affects all subsequent query output
- Can be called with no parameter (toggles current setting) or with "on"/"off" parameter
- Part of psql's comprehensive output formatting system
- Affects both regular query results and meta-command output that displays tabular data
- The setting persists for the duration of the psql session unless changed again
- Source code location: src/bin/psql/command.c:2605-2626