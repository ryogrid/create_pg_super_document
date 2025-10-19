# exec_command_shell_escape

## Location
[src/bin/psql/command.c:3050-3071](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L3050-L3071)

## Overview
Executes shell commands invoked through the `\!` psql meta-command, providing a way to run external commands from within the PostgreSQL interactive terminal.

## Definition
```c
static backslashResult exec_command_shell_escape(PsqlScanState scan_state, bool active_branch)
```

## Detailed Description
This function handles the execution of the `\!` psql meta-command which allows users to execute shell commands from within the psql interactive session. The function operates conditionally based on the `active_branch` parameter - when true, it parses the command line to extract the shell command and executes it via the `do_shell()` function. When `active_branch` is false (typically in conditional blocks that should not execute), it simply ignores the command line without processing it.

The function follows the standard psql command processing pattern, returning appropriate status codes to indicate success or failure of the operation.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer containing the current parsing state and input buffer for extracting the shell command
- `active_branch`: Boolean flag indicating whether the command should actually be executed (true) or just skipped (false)

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option (to extract the shell command from input)
  - [do_shell](../d/do_shell.md) (to actually execute the extracted shell command)
  - [ignore_slash_whole_line](../i/ignore_slash_whole_line.md) (to skip processing when not in active branch)
  - free (to deallocate the extracted command string)
- Called from (representative examples):
  - [exec_command](exec_command.md) (main command dispatcher for psql meta-commands)

## Notes and Other Information
- Returns PSQL_CMD_SKIP_LINE on successful execution or PSQL_CMD_ERROR on failure
- Uses OT_WHOLE_LINE option type when extracting the command, meaning it captures the entire remainder of the line as the shell command
- The function properly handles memory management by freeing the allocated command string after execution
- Part of the psql meta-command processing system, specifically handling the `\!` escape to shell functionality

## Simplified Source

```c
static backslashResult
exec_command_shell_escape(PsqlScanState scan_state, bool active_branch)
{
    bool success = true;

    if (active_branch) {
        // Parse entire command line as shell command
        char *opt = psql_scan_slash_option(scan_state, OT_WHOLE_LINE, NULL, false);

        // Execute shell command
        success = do_shell(opt);
        free(opt);
    }
    else {
        ignore_slash_whole_line(scan_state);
    }

    return success ? PSQL_CMD_SKIP_LINE : PSQL_CMD_ERROR;
}
```