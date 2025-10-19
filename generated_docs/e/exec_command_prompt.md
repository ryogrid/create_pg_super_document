# exec_command_prompt

## Location
[src/bin/psql/command.c:2201-2277](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L2201-L2277)

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
  - `[simple_prompt_extended](../s/simple_prompt_extended.md)()` - Prompts for user input interactively with SIGINT support
  - [gets_fromFile](../g/gets_fromFile.md)() - Reads input from file when not in interactive mode
  - `[SetVariable](../S/SetVariable.md)()` - Sets the psql variable with the input value
  - `fputs()`, `fflush()` - Standard I/O functions for displaying prompt text
  - `free()` - Memory management
  - `pg_log_error()` - Error logging
  - [ignore_slash_options](../i/ignore_slash_options.md)() - Handles unused options when inactive
- Called from (representative examples):
  - [exec_command](exec_command.md) - Main command dispatcher in psql

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

## Simplified Source

```c
static backslashResult exec_command_prompt(PsqlScanState scan_state, bool active_branch, const char *cmd) {
    if (!active_branch) {
        ignore_slash_options(scan_state);
        return PSQL_CMD_SKIP_LINE;
    }

    // Parse arguments: either "\prompt variable" or "\prompt prompt_text variable"
    char *arg1 = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false);
    char *arg2 = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, false);

    if (!arg1) {
        pg_log_error("\\%s: missing required argument", cmd);
        return PSQL_CMD_ERROR;
    }

    char *prompt_text = NULL;
    char *variable_name = arg1;

    // If two args provided, first is prompt text, second is variable
    if (arg2) {
        prompt_text = arg1;
        variable_name = arg2;
    }

    char *result;
    bool success = true;

    // Handle interactive vs file input
    if (!pset.inputfile) {
        // Interactive mode: use SIGINT-aware prompting
        PromptInterruptContext prompt_ctx = {
            .jmpbuf = sigint_interrupt_jmp,
            .enabled = &sigint_interrupt_enabled,
            .canceled = false
        };
        result = simple_prompt_extended(prompt_text, true, &prompt_ctx);

        if (prompt_ctx.canceled) {
            success = false;
        }
    } else {
        // File mode: display prompt and read from stdin
        if (prompt_text) {
            fputs(prompt_text, stdout);
            fflush(stdout);
        }
        result = gets_fromFile(stdin);
        if (!result) {
            pg_log_error("\\%s: could not read value for variable", cmd);
            success = false;
        }
    }

    // Set the variable if we got a result
    if (success && result && !SetVariable(pset.vars, variable_name, result)) {
        success = false;
    }

    free(result);
    free(arg1);
    free(arg2);

    return success ? PSQL_CMD_SKIP_LINE : PSQL_CMD_ERROR;
}
```