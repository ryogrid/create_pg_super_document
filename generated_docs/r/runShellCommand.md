# runShellCommand

## Location
src/bin/pgbench/pgbench.c: 2922 - 3027

## Overview
Executes a shell command constructed from argument variables and optionally captures its integer output to assign to a pgbench variable.

## Definition


## Detailed Description
The `runShellCommand` function constructs and executes shell commands in pgbench scripts with sophisticated argument processing and variable substitution. It supports three types of arguments: string literals, variable references (prefixed with ':'), and escaped colons ('::' for literal ':'). The function has two execution modes: a fast path for commands without output capture using `system()`, and a capture mode using `popen()` that reads the command's standard output and converts it to an integer for variable assignment. The command construction enforces size limits and provides comprehensive error handling for various failure scenarios including command launch failures, read errors, and invalid output formats.

Key features include:
- **Variable substitution**: Arguments starting with ':' are treated as variable references
- **Size validation**: Commands are limited by SHELL_COMMAND_SIZE to prevent overflow
- **Output capture**: When a variable name is provided, captures and parses integer output
- **Error handling**: Comprehensive error reporting for all failure modes

## Parameters / Member Variables
- `variables`: Pointer to the variable collection used for variable lookup and assignment
- `variable`: Target variable name for storing command output (NULL for commands without output capture)
- `argv`: Array of argument strings that will be processed and joined into the command
- `argc`: Number of arguments in the argv array

## Dependencies
- Functions called/Symbols referenced:
  - getVariable
  - putVariableInt
  - system
  - popen
  - pclose
  - fgets
  - strtol
  - strlen
  - memcpy
  - fflush
  - isspace
  - pg_log_error
  - pg_log_debug
- Types used:
  - Variables
  - FILE
- Constants used:
  - SHELL_COMMAND_SIZE
- Global variables referenced:
  - timer_exceeded
- Called from (representative examples):
  - executeMetaCommand

## Notes and Other Information
- The function is declared as static, indicating it's for internal use within the pgbench module
- Supports special argument syntax: ':var' for variables, '::name' for literal strings starting with ':'
- Command size is limited to prevent buffer overflow attacks
- Only integer outputs are supported for variable assignment - string outputs cause errors
- Uses `fflush(NULL)` before command execution to ensure proper output synchronization
- The fast path (`system()`) is used when no output capture is needed for better performance
- Error messages respect the timer_exceeded flag to avoid noise during benchmark timeouts
- Debug logging provides visibility into successful variable assignments