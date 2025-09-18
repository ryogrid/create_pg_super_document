# exec_command_dfo

## Location
src/bin/psql/command.c: 1039 - 1080

## Overview
Handles the \df (describe functions) and \do (describe operators) commands in psql, supporting argument pattern matching for more precise filtering of functions and operators.

## Definition
```c
static bool exec_command_dfo(PsqlScanState scan_state, const char *cmd, const char *pattern, bool show_verbose, bool show_system)
```

## Detailed Description
This function is a specialized handler extracted from exec_command_d to manage the complexity of \df and \do commands. It supports advanced pattern matching by allowing users to specify not just the function/operator name pattern, but also argument type patterns. The function collects multiple argument patterns from the command line and passes them to the appropriate describe function (describeFunctions for \df, describeOperators for \do). This enables precise filtering such as \df myfunction int4 text to find functions named 'myfunction' that take integer and text parameters.

## Parameters / Member Variables
- `scan_state`: PsqlScanState pointer for parsing additional argument patterns from the command line
- `cmd`: Command string to determine whether this is \df (functions) or \do (operators)
- `pattern`: Primary name pattern for filtering functions/operators
- `show_verbose`: Boolean flag to show additional details in the output
- `show_system`: Boolean flag to include system-defined objects in the results

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option
  - [describeFunctions](../d/describeFunctions.md)
  - [describeOperators](../d/describeOperators.md)
  - FUNC_MAX_ARGS (constant)
- Called from (representative examples):
  - [exec_command_d](exec_command_d.md) (for \df and \do command variants)

## Notes and Other Information
- Supports up to FUNC_MAX_ARGS argument patterns to prevent buffer overflow
- Automatically frees all allocated argument pattern strings before returning
- The cmd[1] check determines function vs operator handling ('f' for functions, 'o' for operators)
- Returns boolean success status rather than backslashResult like most other command handlers
- Part of psql's advanced introspection capabilities for finding specific function/operator signatures