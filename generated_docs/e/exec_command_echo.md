# exec_command_echo

## Location
[src/bin/psql/command.c:1293-1337](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L1293-L1337)

## Overview
Implements the \echo, \qecho, and \warn commands in psql, which output text to different streams (stdout, query output file, or stderr respectively).

## Definition

```c
static backslashResult
exec_command_echo(PsqlScanState scan_state, bool active_branch, const char *cmd)
```
## Detailed Description
This function handles three different echo-style commands in psql:
- \echo: outputs to stdout
- \qecho: outputs to the query output file (pset.queryFout)  
- \warn: outputs to stderr

The function processes command arguments, handling the special "-n" flag to suppress the trailing newline. Arguments are separated by spaces in the output. If the command is executed within an inactive conditional branch, the arguments are simply ignored.

## Parameters / Member Variables
- `scan_state`: Scanner state for parsing the command line arguments
- `active_branch`: Boolean indicating whether this command should be executed (used for conditional processing)
- `*cmd`: String identifying which specific command is being executed ("echo", "qecho", or "warn")
## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option
  - [ignore_slash_options](../i/ignore_slash_options.md)
  - [PsqlScanState](../P/PsqlScanState.md) (type)
  - OT_NORMAL (constant)
  - PSQL_CMD_SKIP_LINE (return value)
  - [backslashResult](../b/backslashResult.md) (return type)
- Called from (representative examples):
  - [exec_command](exec_command.md)
  - EditableObjectType (indirectly through command dispatch)

## Notes and Other Information
- The "-n" flag must be the first unquoted argument to suppress the newline
- Arguments are processed sequentially and output with spaces between them
- Memory allocated for argument values is properly freed after use
- The function always returns PSQL_CMD_SKIP_LINE to indicate the command line should be skipped from further processing
- When active_branch is false, arguments are consumed but not processed (for conditional command handling)

## Simplified Source

```c
static backslashResult
exec_command_echo(PsqlScanState scan_state, bool active_branch, const char *cmd)
{
    if (active_branch) {
        char *value;
        char quoted;
        bool no_newline = false;
        bool first = true;
        FILE *fout;

        // Determine output stream based on command
        if (strcmp(cmd, "qecho") == 0) {
            fout = pset.queryFout;  // Query output file
        } else if (strcmp(cmd, "warn") == 0) {
            fout = stderr;          // Standard error
        } else {
            fout = stdout;          // Standard output (\echo)
        }

        // Process all arguments
        while ((value = psql_scan_slash_option(scan_state, OT_NORMAL, &quoted, false))) {
            // Check for -n flag (no newline) as first unquoted argument
            if (first && !no_newline && !quoted && strcmp(value, "-n") == 0) {
                no_newline = true;
            } else {
                // Output space before argument (except first)
                if (!first) {
                    fputc(' ', fout);
                }
                fputs(value, fout);
                first = false;
            }
            free(value);
        }

        // Add newline unless suppressed by -n flag
        if (!no_newline) {
            fputs("\n", fout);
        }
    } else {
        ignore_slash_options(scan_state);
    }

    return PSQL_CMD_SKIP_LINE;
}
```