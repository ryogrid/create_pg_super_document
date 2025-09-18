# exec_command_reset

## Location
src/bin/psql/command.c: 2347 - 2364

## Overview
Implements the psql \r backslash command that clears and resets the query buffer to its initial empty state.

## Definition


## Detailed Description
The  function handles the execution of the \r backslash command in psql. This command provides users with a way to clear the current query buffer completely, effectively starting fresh with an empty buffer. The function operates only when the current branch is active (controlled by conditional execution logic in psql), ensuring that reset operations are only performed when appropriate in the command flow.

When executed, the function performs two main operations: it resets the query buffer to an empty state using  and resets the scanner state using . If psql is not running in quiet mode, it provides user feedback by displaying a confirmation message that the query buffer has been cleared.

## Parameters / Member Variables
- : Scanner state object that tracks the current parsing state and needs to be reset along with the query buffer
- : Boolean flag indicating whether this command should actually execute (used for conditional execution in psql)
- : The PQExpBuffer containing the current query text that will be cleared

## Dependencies
- Functions called/Symbols referenced:
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md)
  - psql_scan_reset
  - [PsqlScanState](../P/PsqlScanState.md) (type)
  - [backslashResult](../b/backslashResult.md) (return type)
  - PSQL_CMD_SKIP_LINE (return value)
- Called from (representative examples):
  - [exec_command](exec_command.md)
  - EditableObjectType (indirectly through command dispatch)

## Notes and Other Information
- This function is part of psql's backslash command system, specifically handling the \r reset command
- The function respects the  setting, only displaying confirmation messages when not in quiet mode  
- Returns  to indicate that the current input line should be skipped after processing
- The  parameter enables conditional execution, allowing commands to be parsed but not executed in certain contexts (such as within false branches of \if constructs)