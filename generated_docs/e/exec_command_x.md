# exec_command_x

## Location
src/bin/psql/command.c: 3000 - 3021

## Overview
Implements the psql \x command, which sets or toggles the expanded table display format for query results.

## Definition
static backslashResult exec_command_x(PsqlScanState scan_state, bool active_branch)

## Detailed Description
This function handles the execution of the \x command in psql, which controls the expanded table display mode. In expanded mode, query results are displayed vertically with each column on a separate line rather than in traditional tabular format. The function can either toggle the current setting (when no argument is provided) or set it to a specific value (on/off). This command is particularly useful for wide tables that don't fit well in standard horizontal display format.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer used for parsing command arguments
- `active_branch`: Boolean indicating whether the command should be executed (true) or just parsed (false)

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option: Parses the optional on/off argument
  - do_pset: Core function that handles setting psql display options
  - [ignore_slash_options](../i/ignore_slash_options.md): Skips parsing when in inactive branch
- Called from (representative examples):
  - [exec_command](exec_command.md): Main command dispatcher in psql

## Notes and Other Information
- When called without arguments, toggles the current expanded display setting
- When called with argument (on/off/true/false), sets the expanded display to that value
- Uses the standard do_pset mechanism for consistent option handling across psql
- Setting affects all subsequent query results until changed again
- Returns PSQL_CMD_SKIP_LINE on success, PSQL_CMD_ERROR on failure
- Memory management: Properly frees the allocated option string
- Part of psql's comprehensive display formatting system controlled via pset.popt