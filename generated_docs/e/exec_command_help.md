# exec_command_help

## Location
src/bin/psql/command.c: 1663 - 1682

## Overview
Implements the \help command in PostgreSQL's psql client, providing SQL command help to users.

## Definition


## Detailed Description
This function handles the execution of the \help backslash command in psql. When invoked, it extracts the optional topic parameter from the command line and calls the helpSQL function to display help information about SQL commands. The function respects the active_branch parameter for conditional execution in psql scripts (used with \if constructs). If the command is not in an active branch, it simply ignores the remainder of the line without processing.

## Parameters / Member Variables
- : PsqlScanState pointer that tracks the current parsing state of the psql input
- : Boolean flag indicating whether this command is being executed in an active conditional branch

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option (extracts command arguments from input)
  - helpSQL (displays SQL command help)
  - ignore_slash_whole_line (skips command processing when in inactive branch)
  - free (deallocates memory for the option string)
- Called from (representative examples):
  - exec_command (main command dispatcher in psql)

## Notes and Other Information
- Returns PSQL_CMD_SKIP_LINE to indicate the entire line has been processed
- Uses OT_WHOLE_LINE option type to capture the entire remainder of the line as the help topic
- Integrates with psql's pager settings (pset.popt.topt.pager) for output formatting
- Part of psql's backslash command infrastructure for interactive SQL operations