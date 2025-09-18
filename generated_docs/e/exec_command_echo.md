# exec_command_echo

## Location
src/bin/psql/command.c: 1293 - 1337

## Overview
Implements the \echo, \qecho, and \warn commands in psql, which output text to different streams (stdout, query output file, or stderr respectively).

## Definition


## Detailed Description
This function handles three different echo-style commands in psql:
- \echo: outputs to stdout
- \qecho: outputs to the query output file (pset.queryFout)  
- \warn: outputs to stderr

The function processes command arguments, handling the special "-n" flag to suppress the trailing newline. Arguments are separated by spaces in the output. If the command is executed within an inactive conditional branch, the arguments are simply ignored.

## Parameters / Member Variables
- : Scanner state for parsing the command line arguments
- : Boolean indicating whether this command should be executed (used for conditional processing)
- : String identifying which specific command is being executed ("echo", "qecho", or "warn")

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option
  - [ignore_slash_options](../i/ignore_slash_options.md)
  - [PsqlScanState](../P/PsqlScanState.md) (type)
  - OT_NORMAL (constant)
  - PSQL_CMD_SKIP_LINE (return value)
  - [backslashResult](../b/backslashResult.md) (return type)
- Called from (representative examples):
  - [exec_command](exec_command.md)
  - EditableObjectType (indirectly through command dispatch)

## Notes and Other Information
- The "-n" flag must be the first unquoted argument to suppress the newline
- Arguments are processed sequentially and output with spaces between them
- Memory allocated for argument values is properly freed after use
- The function always returns PSQL_CMD_SKIP_LINE to indicate the command line should be skipped from further processing
- When active_branch is false, arguments are consumed but not processed (for conditional command handling)