# exec_command_dfo

## Location
[src/bin/psql/command.c:1039-1080](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L1039-L1080)

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

## Simplified Source

```c
static bool
exec_command_dfo(PsqlScanState scan_state, const char *cmd, const char *pattern,
                 bool show_verbose, bool show_system)
{
    bool success;
    char *arg_patterns[FUNC_MAX_ARGS];
    int num_arg_patterns = 0;

    // Collect argument type patterns for precise filtering
    if (pattern) {
        char *ap;
        while ((ap = psql_scan_slash_option(scan_state, OT_NORMAL, NULL, true)) != NULL) {
            arg_patterns[num_arg_patterns++] = ap;
            if (num_arg_patterns >= FUNC_MAX_ARGS)
                break;  // Prevent buffer overflow
        }
    }

    // Dispatch to appropriate describe function
    if (cmd[1] == 'f') {
        // \df - describe functions
        success = describeFunctions(&cmd[2], pattern, arg_patterns,
                                  num_arg_patterns, show_verbose, show_system);
    } else {
        // \do - describe operators
        success = describeOperators(pattern, arg_patterns, num_arg_patterns,
                                  show_verbose, show_system);
    }

    // Clean up allocated argument patterns
    while (--num_arg_patterns >= 0) {
        free(arg_patterns[num_arg_patterns]);
    }

    return success;
}
```