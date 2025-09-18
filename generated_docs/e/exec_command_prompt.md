# exec_command_prompt

## Location
src/bin/psql/command.c: 2201 - 2277

## Overview
Implements the PostgreSQL psql `\prompt` command that interactively prompts the user for input and stores the result in a psql variable.

## Definition
```c
static backslashResult exec_command_prompt(PsqlScanState scan_state, bool active_branch, const char *cmd)
```

## Detailed Description
The `exec_command_prompt` function handles the `\prompt` backslash command in psql, which allows scripts and interactive sessions to prompt users for input and store that input in psql variables. The command supports two forms: `\prompt variable` (prompts with no text) and `\prompt prompt_text variable` (prompts with custom text). When reading from a file instead of interactive input, it displays the prompt text and reads from stdin. The function includes proper SIGINT handling to allow users to cancel the prompt, and validates that the required variable name argument is provided.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer for parsing command line arguments and options
- `active_branch`: Boolean indicating if the command should be executed (used for conditional execution in psql scripts)
- `cmd`: String containing the command name (used for error messages)

## Dependencies
- Functions called/Symbols referenced:
  - `psql_scan_slash_option()` - Parses command arguments (prompt text and variable name)
  - `simple_prompt_extended()` - Prompts for user input interactively with SIGINT support
  - `[gets_fromFile](../g/gets_fromFile.md)()` - Reads input from file when not in interactive mode
  - `SetVariable()` - Sets the psql variable with the input value
  - `fputs()`, `fflush()` - Standard I/O functions for displaying prompt text
  - `free()` - Memory management
  - `pg_log_error()` - Error logging
  - `[ignore_slash_options](../i/ignore_slash_options.md)()` - Handles unused options when inactive
- Called from (representative examples):
  - `[exec_command](exec_command.md)` - Main command dispatcher in psql

## Notes and Other Information
- Returns `PSQL_CMD_SKIP_LINE` on success, `PSQL_CMD_ERROR` on failure
- Supports two argument forms: `\prompt variable` and `\prompt prompt_text variable`
- Handles both interactive input (via `simple_prompt_extended`) and file input (via `gets_fromFile`)
- Supports SIGINT cancellation during prompting through PromptInterruptContext
- When reading from file, displays prompt text to stdout before reading from stdin
- Only executes when `active_branch` is true, supporting conditional execution in psql scripts
- Properly validates that required variable name argument is provided
- Located in `src/bin/psql/command.c:2201-2277`
- Essential for creating interactive psql scripts that need user input