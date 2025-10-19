# exec_command

## Location
[src/bin/psql/command.c:305-465](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L305-L465)

## Overview
exec_command is a comprehensive command dispatcher that executes individual PostgreSQL psql backslash commands by matching command names to their respective handler functions.

## Definition

```c
static backslashResult
exec_command(const char *cmd,
			 PsqlScanState scan_state,
			 ConditionalStack cstack,
			 PQExpBuffer query_buf,
			 PQExpBuffer previous_buf)
```
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
  - [conditional_active](../c/conditional_active.md)
  - [is_branching_command](../i/is_branching_command.md)
  - pg_log_warning
  - [exec_command_a](exec_command_a.md), exec_command_bind, exec_command_C (and many other command handlers)
  - [copy_previous_query](../c/copy_previous_query.md)
- Called from (representative examples):
  - [HandleSlashCmds](../H/HandleSlashCmds.md) (src/bin/psql/command.c:248)

## Notes and Other Information
- Returns backslashResult status codes: typically PSQL_CMD_SKIP_LINE for success, PSQL_CMD_ERROR for failure, or PSQL_CMD_UNKNOWN for unrecognized commands
- Commands executed within false conditional branches are warned about but still parsed to consume arguments properly
- Supports over 50 different backslash commands including database operations (\c, \l), formatting (\x, \H), conditionals (\if, \elif), and utilities (\!, \?)
- Handles both single-character and multi-character command aliases (e.g., \c and \connect)
- Special case handling for commands returning PSQL_CMD_SEND to automatically copy previous query if current query buffer is empty

## Simplified Source

```c
static backslashResult exec_command(const char *cmd,
                                   PsqlScanState scan_state,
                                   ConditionalStack cstack,
                                   PQExpBuffer query_buf,
                                   PQExpBuffer previous_buf) {
    backslashResult status;
    bool active_branch = conditional_active(cstack);

    // Warn about commands in false \if-branches (interactive mode only)
    if (pset.cur_cmd_interactive && !active_branch && !is_branching_command(cmd)) {
        pg_log_warning("\\%s command ignored; use \\endif or Ctrl-C to exit current \\if block", cmd);
    }

    // Command dispatch - match command names to handlers
    if (strcmp(cmd, "a") == 0)
        status = exec_command_a(scan_state, active_branch);
    else if (strcmp(cmd, "bind") == 0)
        status = exec_command_bind(scan_state, active_branch);
    else if (strcmp(cmd, "C") == 0)
        status = exec_command_C(scan_state, active_branch);
    else if (strcmp(cmd, "c") == 0 || strcmp(cmd, "connect") == 0)
        status = exec_command_connect(scan_state, active_branch);
    // ... [many more command handlers for cd, conninfo, copy, d, edit, etc.] ...
    else if (strcmp(cmd, "q") == 0 || strcmp(cmd, "quit") == 0)
        status = exec_command_quit(scan_state, active_branch);
    else if (strcmp(cmd, "!") == 0)
        status = exec_command_shell_escape(scan_state, active_branch);
    else if (strcmp(cmd, "?") == 0)
        status = exec_command_slash_command_help(scan_state, active_branch);
    else
        status = PSQL_CMD_UNKNOWN;

    // Special handling: copy previous query if current buffer is empty
    if (status == PSQL_CMD_SEND) {
        copy_previous_query(query_buf, previous_buf);
    }

    return status;
}
```