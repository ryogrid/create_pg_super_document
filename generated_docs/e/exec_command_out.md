# exec_command_out

## Location
[src/bin/psql/command.c:2076-2098](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L2076-L2098)

## Overview
Handles the \\o command in psql, which redirects query output to a file or pipe instead of the standard display.

## Definition
```c
static backslashResult exec_command_out(PsqlScanState scan_state, bool active_branch)
```

## Detailed Description
The `exec_command_out` function implements the \\o (output) command that allows users to redirect the output of subsequent SQL queries to a file or pipe instead of displaying them in the terminal. This is particularly useful for saving query results, creating reports, or piping output to other programs for further processing.

The function supports:
- File output by specifying a filename (creates or overwrites the file)
- Pipe output by prefixing with | (pipes to shell command)  
- Returning to stdout by providing no argument or empty string
- Tilde expansion for home directory references in file paths

When executed within a conditional block, the function respects the active_branch parameter and only changes output redirection when the condition is true. Otherwise, it consumes the file/pipe argument without taking action.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer for parsing the filename or pipe command argument
- `active_branch`: Boolean indicating whether this command should execute or be skipped (for conditional execution)

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option
  - [expand_tilde](expand_tilde.md)
  - [setQFout](../s/setQFout.md)
  - free
  - [ignore_slash_filepipe](../i/ignore_slash_filepipe.md)
- Called from (representative examples):
  - [exec_command](exec_command.md)

## Notes and Other Information
- Returns PSQL_CMD_SKIP_LINE on success or PSQL_CMD_ERROR on failure
- Part of psql's output control system for redirecting query results
- Uses OT_FILEPIPE option type to handle both file paths and pipe commands
- Supports tilde (~) expansion for home directory references in file paths
- Memory management includes proper cleanup of the filename string
- When no argument is provided, it resets output to stdout (terminal)
- Integrates with conditional execution system via active_branch parameter
- Uses ignore_slash_filepipe() to consume unused arguments when not executing
- The output redirection persists until changed by another \\o command or psql exit

## Simplified Source

```c
static backslashResult exec_command_out(PsqlScanState scan_state, bool active_branch) {
    if (active_branch) {
        // Parse optional filename or pipe command
        char *fname = psql_scan_slash_option(scan_state, OT_FILEPIPE, NULL, true);

        // Handle tilde expansion for home directory
        expand_tilde(&fname);

        // Set query output destination (file, pipe, or stdout)
        bool success = setQFout(fname);

        free(fname);
        return success ? PSQL_CMD_SKIP_LINE : PSQL_CMD_ERROR;
    } else {
        // Skip execution in inactive conditional branch
        ignore_slash_filepipe(scan_state);
        return PSQL_CMD_SKIP_LINE;
    }
}
```