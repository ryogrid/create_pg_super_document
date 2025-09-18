# ignore_slash_whole_line

## Location
src/bin/psql/command.c: 3243 - 3254

## Overview
This function reads and discards whole-line slash command arguments from the input stream, ensuring consistent parsing behavior between active and inactive conditional branches for commands that consume entire lines.

## Definition
```c
static void ignore_slash_whole_line(PsqlScanState scan_state)
```

## Detailed Description
The `ignore_slash_whole_line` function is a specialized component of psql's conditional command processing system that handles slash commands taking OT_WHOLE_LINE option types during inactive-branch processing. Like `ignore_slash_filepipe`, this function is marked as *MUST* be used to ensure parsing consistency.

The function addresses the specific challenge of whole-line arguments, which consume all remaining text on the current input line. This type of argument parsing can vary significantly in the amount of text consumed depending on what follows the command, making it critical that the same parsing behavior occurs in both active and inactive branches.

The function makes a single call to `psql_scan_slash_option()` with the OT_WHOLE_LINE option type, which reads from the current position to the end of the line, then immediately frees the result without processing it. Notably, the function uses a hardcoded `false` value for the semicolon parameter, as mentioned in the comment - the semicolon setting doesn't affect the amount of input text consumed, so there's no need to duplicate the caller's semicolon parameter.

## Parameters / Member Variables
- `scan_state`: A `PsqlScanState` structure that maintains the current state of the psql command scanner, including the input buffer and parsing position

## Dependencies
- Functions called/Symbols referenced:
  - `psql_scan_slash_option`: Scans for the next slash command option from the input with specified type
  - `[OT_WHOLE_LINE](../O/OT_WHOLE_LINE.md)`: Option type constant for whole-line command arguments
  - `[PsqlScanState](../P/PsqlScanState.md)`: Scanner state structure type

- Called from (representative examples):
  - `[exec_command_copy](../e/exec_command_copy.md)`: When \copy commands are in inactive branches
  - `[exec_command_help](../e/exec_command_help.md)`: When \help commands are in inactive branches  
  - `[exec_command_shell_escape](../e/exec_command_shell_escape.md)`: When shell escape commands are in inactive branches
  - `[exec_command_ef_ev](../e/exec_command_ef_ev.md)`: When \ef/\ev commands are in inactive branches

## Notes and Other Information
- This function is essential for maintaining parser state consistency when processing whole-line arguments
- The semicolon parameter behavior is simplified since it doesn't affect text consumption amounts
- Whole-line arguments are commonly used for commands that take SQL statements, file paths, or shell commands as arguments
- The function is static to the command.c file, indicating it's an internal utility for command processing
- Memory management is handled properly by immediately freeing the argument string after reading
- The *MUST* designation indicates critical importance for correct conditional processing behavior