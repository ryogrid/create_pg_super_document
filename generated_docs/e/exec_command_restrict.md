# exec_command_restrict

## Location
[src/bin/psql/command.c:2365-2393](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L2365-L2393)

## Overview
Implements the psql \restrict backslash command that enables restricted mode with a specified security key to limit command execution.

## Definition


## Detailed Description
The  function handles the execution of the \restrict backslash command in psql. This command enables a security feature called "restricted mode" where psql operations are limited until the mode is disabled with the correct key. The function requires a security key argument that will be used later to exit restricted mode.

When executed in an active branch, the function parses the required key argument using the scanner, validates that a non-empty key was provided, and then sets the global  flag to true while storing the provided key in . If no key is provided or the key is empty, the function returns an error. In inactive branches, the function simply ignores any slash options to maintain parser consistency.

## Parameters / Member Variables
- : Scanner state object used to parse the key argument from the command line
- : Boolean flag indicating whether this command should actually execute (used for conditional execution in psql)
- : The name of the command being executed (used for error reporting)

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option
  - [pstrdup](../p/pstrdup.md)
  - pg_log_error
  - [ignore_slash_options](../i/ignore_slash_options.md)
  - [PsqlScanState](../P/PsqlScanState.md) (type)
  - [backslashResult](../b/backslashResult.md) (return type)
  - OT_NORMAL (option type)
  - PSQL_CMD_ERROR (error return value)
  - PSQL_CMD_SKIP_LINE (success return value)
- Called from (representative examples):
  - [exec_command](exec_command.md)
  - EditableObjectType (indirectly through command dispatch)

## Notes and Other Information
- This function is part of psql's security infrastructure, implementing a restricted mode feature
- The function uses  to ensure that restricted mode is not already active when trying to enter it
- The security key is stored globally in  and the restricted state in the  variable
- Error handling includes validation that a non-empty key argument is provided
- The function properly handles conditional execution by ignoring options when not in an active branch
- Returns  if the required key argument is missing or empty, otherwise returns 