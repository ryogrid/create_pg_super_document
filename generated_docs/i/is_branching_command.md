# is_branching_command

## Location
src/bin/psql/command.c: 3255 - 3270

## Overview
This function determines whether a given psql command string represents a conditional branching command used in psql's if-elif-else-endif control flow.

## Definition
```c
static bool is_branching_command(const char *cmd)
```

## Detailed Description
The `is_branching_command` function is a simple utility that identifies conditional branching commands in psql's command processing system. It performs string comparisons to determine if the provided command is one of the four conditional control flow commands: "if", "elif", "else", or "endif".

This function plays a crucial role in psql's interactive mode behavior. When processing commands within an inactive conditional branch (where the condition evaluated to false), psql normally warns users that commands are being ignored. However, branching commands themselves should not trigger these warnings since they are part of the conditional control structure and need to be processed regardless of the current branch state.

The function uses simple string comparisons with `strcmp()` to check each of the four possible branching commands. It returns true if the command matches any of these, and false otherwise.

## Parameters / Member Variables
- `cmd`: A null-terminated string containing the psql command name to check (without the leading backslash)

## Dependencies
- Functions called/Symbols referenced:
  - `strcmp`: Standard C library string comparison function

- Called from (representative examples):
  - `[exec_command](../e/exec_command.md)`: Used to determine whether to show "command ignored" warnings in interactive mode when processing commands in inactive conditional branches

## Notes and Other Information
- The function is static to the command.c file, indicating it's an internal utility for command processing
- This function is essential for proper user experience in interactive mode, preventing confusing warnings for legitimate conditional commands
- The function only checks the command name itself, not any parameters or arguments that might follow
- Used specifically in the context of warning suppression - branching commands are always processed regardless of conditional state
- Part of psql's conditional processing infrastructure that supports nested if-elif-else-endif blocks
- The four recognized branching commands correspond directly to psql's conditional control flow syntax