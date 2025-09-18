# HandleSlashCmds

## Location
[src/bin/psql/command.c:221-304](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L221-L304)

## Overview
HandleSlashCmds is the main dispatcher function that processes all PostgreSQL psql backslash commands (commands starting with '\'). It serves as the central entry point for executing psql meta-commands.

## Definition


## Detailed Description
HandleSlashCmds parses and executes backslash commands in the PostgreSQL psql client. The function first extracts the command name using psql_scan_slash_command(), then delegates actual command execution to exec_command(). It handles error conditions, manages restricted mode (where only \unrestrict is allowed), and performs cleanup operations like consuming remaining arguments and handling trailing backslashes.

The function includes sophisticated argument handling - after successful command execution, it consumes any extra arguments and warns about them, while for failed commands it silently discards the rest of the line. It also manages conditional stack states to properly handle backtick evaluation in arguments.

## Parameters / Member Variables
- `scan_state`: Lexer working state positioned just after the '\' character, advanced past the command and arguments on return
- `cstack`: Current conditional stack state (\if stack), examined and potentially modified by conditional commands
- `query_buf`: Current query buffer that may be modified by command execution (e.g., \r clears it)
- `previous_buf`: Most recently sent query buffer (read-only), copied by some commands into query_buf

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_command
  - [exec_command](../e/exec_command.md)
  - [conditional_active](../c/conditional_active.md)
  - [conditional_stack_push](../c/conditional_stack_push.md)
  - [conditional_stack_pop](../c/conditional_stack_pop.md)
  - psql_scan_slash_option
  - psql_scan_slash_command_end
  - pg_log_error
  - pg_log_error_hint
  - pg_log_warning
- Called from (representative examples):
  - [MainLoop](../M/MainLoop.md) (src/bin/psql/mainloop.c:496)

## Notes and Other Information
- Returns backslashResult status codes indicating desired action (PSQL_CMD_ERROR, PSQL_CMD_UNKNOWN, etc.)
- Handles restricted mode where only \unrestrict command is permitted
- Automatically flushes output to ensure commands that write to queryFout are properly displayed
- Both query_buf and previous_buf can be NULL when executing "-c" command-line options
- Implements proper cleanup by consuming remaining arguments and handling parse state consistently