# exec_command

## Location
src/bin/psql/command.c: 305 - 465

## Overview
exec_command is a comprehensive command dispatcher that executes individual PostgreSQL psql backslash commands by matching command names to their respective handler functions.

## Definition


## Detailed Description
exec_command serves as the central routing function for all psql backslash commands. It uses a large if-else chain to match the incoming command string against known command names and dispatches to the appropriate exec_command_* handler function. The function handles conditional branch logic - when commands are executed within a false \if-branch, it warns users in interactive mode but continues parsing to consume the correct amount of parameter text.

The function implements special handling for commands that return PSQL_CMD_SEND status - these commands want to execute the previous query buffer if the current query buffer is empty, which is handled centrally rather than in individual command subroutines.

## Parameters / Member Variables
- `cmd`: The backslash command name (without the leading backslash) to be executed
- `scan_state`: Lexer working state for parsing command arguments  
- `cstack`: Conditional stack state for handling \if/\elif/\else/\endif logic
- `query_buf`: Current query buffer that may be modified by the command
- `previous_buf`: Most recently executed query buffer (read-only)

## Dependencies
- Functions called/Symbols referenced:
  - conditional_active
  - is_branching_command
  - pg_log_warning
  - exec_command_a, exec_command_bind, exec_command_C (and many other command handlers)
  - copy_previous_query
- Called from (representative examples):
  - HandleSlashCmds (src/bin/psql/command.c:248)

## Notes and Other Information
- Returns backslashResult status codes: typically PSQL_CMD_SKIP_LINE for success, PSQL_CMD_ERROR for failure, or PSQL_CMD_UNKNOWN for unrecognized commands
- Commands executed within false conditional branches are warned about but still parsed to consume arguments properly
- Supports over 50 different backslash commands including database operations (\c, \l), formatting (\x, \H), conditionals (\if, \elif), and utilities (\!, \?)
- Handles both single-character and multi-character command aliases (e.g., \c and \connect)
- Special case handling for commands returning PSQL_CMD_SEND to automatically copy previous query if current query buffer is empty