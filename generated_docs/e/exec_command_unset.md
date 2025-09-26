# exec_command_unset

## Location
[src/bin/psql/command.c:2721-2750](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L2721-L2750)

## Overview
Implements the psql \unset command, which removes a variable from the psql session's variable store.

## Definition
static backslashResult exec_command_unset(PsqlScanState scan_state, bool active_branch, const char *cmd)

## Detailed Description
This function handles the execution of the \unset command in psql, which is used to remove variables that were previously set with \set. The function parses the variable name from the command line and calls SetVariable with a NULL value to effectively unset the variable. The function follows the standard psql command processing pattern, where it only executes when active_branch is true (meaning the command is not within a false conditional branch).

## Parameters / Member Variables
- : PsqlScanState pointer used for parsing command arguments
- : Boolean indicating whether the command should be executed (true) or just parsed (false) 
- : String containing the command name ("unset") for error reporting purposes

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option: Parses the variable name argument
  - [SetVariable](../S/SetVariable.md): Removes the variable by setting it to NULL
  - [ignore_slash_options](../i/ignore_slash_options.md): Skips parsing when in inactive branch
  - pg_log_error: Reports error messages
- Called from (representative examples):
  - [exec_command](exec_command.md): Main command dispatcher in psql

## Notes and Other Information
- Returns PSQL_CMD_SKIP_LINE on success to indicate the command line should be skipped from further processing
- Returns PSQL_CMD_ERROR on failure (missing argument or SetVariable failure)
- Memory management: Properly frees the allocated option string after use
- Error handling: Reports missing argument errors with the command name for context
- The function operates within psql's conditional execution framework