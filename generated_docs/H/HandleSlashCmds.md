# HandleSlashCmds

## Location
[src/bin/psql/command.c:221-304](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L221-L304)

## Overview
HandleSlashCmds is the main dispatcher function that processes all PostgreSQL psql backslash commands (commands starting with '\'). It serves as the central entry point for executing psql meta-commands.

## Definition

```c
backslashResult
HandleSlashCmds(PsqlScanState scan_state,
				ConditionalStack cstack,
				PQExpBuffer query_buf,
				PQExpBuffer previous_buf)
```
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

## Simplified Source

```c
backslashResult HandleSlashCmds(PsqlScanState scan_state,
                               ConditionalStack cstack,
                               PQExpBuffer query_buf,
                               PQExpBuffer previous_buf) {
    backslashResult status;
    char *cmd;
    char *arg;

    // Parse the backslash command name
    cmd = psql_scan_slash_command(scan_state);

    // Check restricted mode - only \unrestrict allowed
    if (restricted && strcmp(cmd, "unrestrict") != 0) {
        pg_log_error("backslash commands are restricted; only \\unrestrict is allowed");
        status = PSQL_CMD_ERROR;
    } else {
        // Execute the command
        status = exec_command(cmd, scan_state, cstack, query_buf, previous_buf);
    }

    // Handle unknown commands
    if (status == PSQL_CMD_UNKNOWN) {
        pg_log_error("invalid command \\%s", cmd);
        if (pset.cur_cmd_interactive) {
            pg_log_error_hint("Try \\? for help.");
        }
        status = PSQL_CMD_ERROR;
    }

    // Clean up remaining arguments
    if (status != PSQL_CMD_ERROR) {
        // Warn about extra arguments after valid commands
        bool active_branch = conditional_active(cstack);
        conditional_stack_push(cstack, IFSTATE_IGNORED);
        while ((arg = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false))) {
            if (active_branch) {
                pg_log_warning("\\%s: extra argument \"%s\" ignored", cmd, arg);
            }
            free(arg);
        }
        conditional_stack_pop(cstack);
    } else {
        // Silently discard rest of line after errors
        while ((arg = psql_scan_slash_option(scan_state, OT_WHOLE_LINE, NULL, false))) {
            free(arg);
        }
    }

    // Handle trailing backslash and cleanup
    psql_scan_slash_command_end(scan_state);
    free(cmd);
    fflush(pset.queryFout);

    return status;
}
```