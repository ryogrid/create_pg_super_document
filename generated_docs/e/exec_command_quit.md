# exec_command_quit

## Location
src/bin/psql/command.c: 2333 - 2346

## Overview
Implements the PostgreSQL psql `\q` and `\quit` commands that terminate the psql session and exit the program.

## Definition
```c
static backslashResult exec_command_quit(PsqlScanState scan_state, bool active_branch)
```

## Detailed Description
The `exec_command_quit` function handles both the `\q` and `\quit` backslash commands in psql, providing a way to cleanly exit the psql session. This is one of the simplest command implementations in psql. When executed in an active branch, it returns `PSQL_CMD_TERMINATE` to signal that the psql main loop should exit. When not in an active branch (such as in a conditional that evaluates to false), it returns `PSQL_CMD_SKIP_LINE` to continue processing without terminating.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer for command line scanning (unused in this function)
- `active_branch`: Boolean indicating if the command should be executed (determines whether psql actually terminates)

## Dependencies
- Functions called/Symbols referenced:
  - `PSQL_CMD_TERMINATE` - Return code indicating psql should terminate
  - `PSQL_CMD_SKIP_LINE` - Return code indicating line should be skipped
  - `[backslashResult](../b/backslashResult.md)` - Enumeration type for command return values
- Called from (representative examples):
  - `[exec_command](exec_command.md)` - Main command dispatcher in psql

## Notes and Other Information
- Returns `PSQL_CMD_TERMINATE` when `active_branch` is true, causing psql to exit
- Returns `PSQL_CMD_SKIP_LINE` when `active_branch` is false, allowing conditional execution
- Does not perform any cleanup operations - [cleanup](../c/cleanup.md) is handled by the main psql loop
- One of the simplest command implementations, consisting of only a conditional return
- Supports both `\q` and `\quit` command variants (handled by command dispatcher)
- Located in `src/bin/psql/command.c:2333-2346`
- Essential for providing a clean exit mechanism from psql sessions