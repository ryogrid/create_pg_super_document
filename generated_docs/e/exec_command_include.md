# exec_command_include

## Location
[src/bin/psql/command.c:1702-1742](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L1702-L1742)

## Overview
Implements the \i and \ir commands in PostgreSQL's psql client for including and executing SQL scripts from files.

## Definition

```c
static backslashResult
exec_command_include(PsqlScanState scan_state, bool active_branch, const char *cmd)
```
## Detailed Description
This function handles the execution of the \i (include) and \ir (include relative) backslash commands in psql. It reads and processes SQL commands from a specified file. The \i command resolves file paths relative to the current working directory, while \ir resolves paths relative to the directory containing the currently executing script. The function validates that a filename argument is provided, expands tilde (~) characters in file paths, and delegates actual file processing to the process_file function. It respects the active_branch parameter for conditional execution.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer that tracks the current parsing state of the psql input
- `active_branch`: Boolean flag indicating whether this command is being executed in an active conditional branch
- `*cmd`: String indicating which variant of the command was used ("i", "include", "ir", or "include_relative")
## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option (extracts filename argument from input)
  - pg_log_error (logs error messages for missing arguments)
  - strcmp (compares command strings to determine relative vs absolute behavior)
  - [expand_tilde](expand_tilde.md) (expands ~ character in file paths)
  - [process_file](../p/process_file.md) (reads and executes SQL from the specified file)
  - [ignore_slash_options](../i/ignore_slash_options.md) (skips processing when in inactive branch)
  - free (deallocates memory for filename string)
- Called from (representative examples):
  - [exec_command](exec_command.md) (main command dispatcher in psql)

## Notes and Other Information
- Returns PSQL_CMD_SKIP_LINE on success or PSQL_CMD_ERROR on failure
- Supports both short forms (\i, \ir) and long forms (\include, \include_relative)
- Uses OT_NORMAL option type to extract the filename argument
- The include_relative flag determines whether file paths are resolved relative to the current script's directory
- Requires exactly one argument (the filename) - reports an error if missing
- Part of psql's script execution infrastructure for modular SQL development

## Simplified Source

```c
// Simplified version of exec_command_include
static backslashResult exec_command_include(PsqlScanState scan_state, bool active_branch, const char *cmd) {
    bool success = true;

    if (active_branch) {
        // Extract filename argument from command line
        char *fname = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, true);

        if (!fname) {
            // Error if no filename provided
            pg_log_error("\\%s: missing required argument", cmd);
            success = false;
        } else {
            // Determine if this is relative include (ir/include_relative)
            bool include_relative = (strcmp(cmd, "ir") == 0 || strcmp(cmd, "include_relative") == 0);

            // Expand ~ in file path and process the file
            expand_tilde(&fname);
            success = (process_file(fname, include_relative) == EXIT_SUCCESS);
            free(fname);
        }
    } else {
        // Skip processing if in inactive conditional branch
        ignore_slash_options(scan_state);
    }

    return success ? PSQL_CMD_SKIP_LINE : PSQL_CMD_ERROR;
}
```