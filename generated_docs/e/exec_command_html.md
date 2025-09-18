# exec_command_html

## Location
src/bin/psql/command.c: 1683 - 1701

## Overview
Implements the \H and \html commands in PostgreSQL's psql client to toggle between HTML and aligned output formats.

## Definition


## Detailed Description
This function handles the execution of the \H (short form) and \html (long form) backslash commands in psql. It toggles the output format between HTML and aligned formatting. When the current format is not HTML, it switches to HTML format. When already in HTML format, it switches back to aligned format. The function respects the active_branch parameter for conditional execution in psql scripts and uses the do_pset function to actually change the formatting setting.

## Parameters / Member Variables
- : PsqlScanState pointer that tracks the current parsing state of the psql input (unused in this function)
- : Boolean flag indicating whether this command is being executed in an active conditional branch

## Dependencies
- Functions called/Symbols referenced:
  - do_pset (modifies psql settings like output format)
  - PRINT_HTML (enum value for HTML output format)
  - PSQL_CMD_SKIP_LINE (successful command completion return value)
  - PSQL_CMD_ERROR (error return value)
- Called from (representative examples):
  - exec_command (main command dispatcher in psql)

## Notes and Other Information
- Returns PSQL_CMD_SKIP_LINE on success or PSQL_CMD_ERROR on failure
- Toggles between 'html' and 'aligned' formats only - does not cycle through other available formats
- Uses pset global variable to access current formatting options and quiet mode setting
- The scan_state parameter is accepted for consistency with other command handlers but not used
- Part of psql's output formatting control system for customizing query result display