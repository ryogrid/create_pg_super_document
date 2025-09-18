# exec_command_timing

## Location
src/bin/psql/command.c: 2649 - 2680

## Overview
Implements the `\timing` psql command that enables or disables the display of query execution timing information.

## Definition
```c
static backslashResult
exec_command_timing(PsqlScanState scan_state, bool active_branch)
```

## Detailed Description
This function handles the `\timing` psql meta-command which controls whether psql displays timing information after each SQL command execution. When timing is enabled, psql shows the elapsed time for each query, which is useful for performance analysis and optimization. The function can accept an optional boolean parameter to explicitly enable or disable timing, or toggle the current setting if no parameter is provided.

The function provides user feedback by displaying the current timing state unless psql is running in quiet mode. The timing setting is stored in the global pset structure and affects all subsequent query executions.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer for parsing command line options
- `active_branch`: Boolean indicating if the command should be executed or skipped

## Dependencies
- Functions called/Symbols referenced:
  - `psql_scan_slash_option` - Parse optional boolean parameter from command line
  - `ParseVariableBool` - Parse and validate boolean value ("on"/"off", "true"/"false", etc.)
  - `ignore_slash_options` - Skip option parsing when not in active branch
- Called from:
  - `exec_command` - Main command dispatcher for `\timing` command

## Notes and Other Information
- When enabled, timing information appears after each SQL command completion
- Timing precision depends on the system's clock resolution
- Useful for performance testing and query optimization
- Can be called with explicit values: `\timing on`, `\timing off`, or `\timing` (toggles)
- Accepts various boolean representations (on/off, true/false, yes/no, 1/0)
- The setting persists for the duration of the psql session unless changed again
- Does not affect the timing of meta-commands, only SQL queries
- Source code location: src/bin/psql/command.c:2649-2680