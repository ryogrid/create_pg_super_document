# runShellCommand

## Location
[src/bin/pgbench/pgbench.c:2922-3027](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L2922-L3027)

## Overview
Executes a shell command constructed from argument variables and optionally captures its integer output to assign to a pgbench variable.

## Definition

```c
static bool
runShellCommand(Variables *variables, char *variable, char **argv, int argc)
```
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
  - [getVariable](../g/getVariable.md)
  - [putVariableInt](../p/putVariableInt.md)
  - system
  - popen
  - [pclose](../p/pclose.md)
  - fgets
  - strtol
  - strlen
  - memcpy
  - fflush
  - isspace
  - pg_log_error
  - pg_log_debug
- Types used:
  - [Variables](../V/Variables.md)
  - FILE
- Constants used:
  - SHELL_COMMAND_SIZE
- Global variables referenced:
  - timer_exceeded
- Called from (representative examples):
  - [executeMetaCommand](../e/executeMetaCommand.md)

## Notes and Other Information
- The function is declared as static, indicating it's for internal use within the pgbench module
- Supports special argument syntax: ':var' for variables, '::name' for literal strings starting with ':'
- [Command](../C/Command.md) size is limited to prevent buffer overflow attacks
- Only integer outputs are supported for variable assignment - string outputs cause errors
- Uses `fflush(NULL)` before command execution to ensure proper output synchronization
- The fast path (`system()`) is used when no output capture is needed for better performance
- Error messages respect the timer_exceeded flag to avoid noise during benchmark timeouts
- Debug logging provides visibility into successful variable assignments

## Simplified Source

```c
static bool runShellCommand(Variables *variables, char *variable, char **argv, int argc) {
    char command[SHELL_COMMAND_SIZE];
    int len = 0;

    // Build command string from arguments with variable substitution
    for (int i = 0; i < argc; i++) {
        char *arg;

        // Process argument based on prefix
        if (argv[i][0] != ':') {
            arg = argv[i];  // String literal
        } else if (argv[i][1] == ':') {
            arg = argv[i] + 1;  // Escaped colon (::name -> :name)
        } else {
            // Variable reference (:varname)
            arg = getVariable(variables, argv[i] + 1);
            if (arg == NULL) {
                pg_log_error("%s: undefined variable \"%s\"", argv[0], argv[i]);
                return false;
            }
        }

        // Check command size limit
        int arglen = strlen(arg);
        if (len + arglen + (i > 0 ? 1 : 0) >= SHELL_COMMAND_SIZE - 1) {
            pg_log_error("%s: shell command is too long", argv[0]);
            return false;
        }

        // Add space separator between arguments
        if (i > 0)
            command[len++] = ' ';

        // Append argument to command
        memcpy(command + len, arg, arglen);
        len += arglen;
    }
    command[len] = '\0';

    fflush(NULL);  // Ensure output is flushed before system call

    // Fast path: execute without capturing output
    if (variable == NULL) {
        if (system(command)) {
            if (!timer_exceeded)
                pg_log_error("%s: could not launch shell command", argv[0]);
            return false;
        }
        return true;
    }

    // Execute command and capture output for variable assignment
    FILE *fp = popen(command, "r");
    if (fp == NULL) {
        pg_log_error("%s: could not launch shell command", argv[0]);
        return false;
    }

    // Read command output
    char res[64];
    if (fgets(res, sizeof(res), fp) == NULL) {
        if (!timer_exceeded)
            pg_log_error("%s: could not read result of shell command", argv[0]);
        pclose(fp);
        return false;
    }

    if (pclose(fp) < 0) {
        pg_log_error("%s: could not run shell command: %m", argv[0]);
        return false;
    }

    // Parse output as integer and assign to variable
    char *endptr;
    int retval = (int) strtol(res, &endptr, 10);

    // Skip trailing whitespace
    while (*endptr != '\0' && isspace((unsigned char) *endptr))
        endptr++;

    // Validate that entire output was a valid integer
    if (*res == '\0' || *endptr != '\0') {
        pg_log_error("%s: shell command must return an integer (not \"%s\")", argv[0], res);
        return false;
    }

    // Store result in variable
    if (!putVariableInt(variables, "setshell", variable, retval))
        return false;

    pg_log_debug("%s: shell parameter name: \"%s\", value: \"%s\"", argv[0], argv[1], res);
    return true;
}
```