# exec_command_getenv

## Location
src/bin/psql/command.c: 1580 - 1616

## Overview
Implements the \getenv command in psql, which retrieves a value from an environment variable and stores it in a psql variable.

## Definition


## Detailed Description
This function handles the \getenv backslash command which takes two arguments: a psql variable name and an environment variable name. It reads the value from the specified environment variable and assigns it to the psql variable. If the environment variable doesn't exist, the psql variable is not set. The function performs argument validation and provides error messages for missing required arguments.

## Parameters / Member Variables
- : Scanner state for reading command arguments from input stream
- : Whether to actually execute the command (true) or just parse arguments (false)
- : The command name for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option
  - getenv
  - SetVariable
  - ignore_slash_options
  - pg_log_error
- Called from (representative examples):
  - exec_command

## Notes and Other Information
- Requires exactly two arguments: psql variable name and environment variable name
- Uses getenv() to read from the system environment
- Only sets the psql variable if the environment variable exists and has a value
- When not in active_branch, uses ignore_slash_options to skip argument parsing
- Returns PSQL_CMD_SKIP_LINE on success or PSQL_CMD_ERROR on failure