# do_shell

## Location
[src/bin/psql/command.c:5282-5332](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L5282-L5332)

## Overview
Executes shell commands from within the PostgreSQL psql client, either launching an interactive shell session or running a specific command.

## Definition

```c
struct itimerval interval;
```
## Detailed Description
The  function provides shell command execution functionality for PostgreSQL's psql client. When called with a command string, it executes that specific command using the system() function. When called with NULL, it launches an interactive shell session using the user's preferred shell (determined by environment variables).

The function handles cross-platform differences between Unix-like systems and Windows for shell invocation. On Unix systems, it uses "exec" to replace the current process, while on Windows it directly invokes the shell with proper quoting. After command execution, it updates psql's shell result variables and provides error handling for failed executions.

## Parameters / Member Variables
- : A string containing the shell command to execute, or NULL to launch an interactive shell session

## Dependencies
- Functions called/Symbols referenced:
  - DEFAULT_SHELL (fallback shell when environment variables are not set)
  - system (standard C library function for executing shell commands)
  - [SetShellResultVariables](../S/SetShellResultVariables.md) (updates psql variables with command execution results)
- Called from (representative examples):
  - [exec_command_shell_escape](../e/exec_command_shell_escape.md) (handles \! commands in psql)

## Notes and Other Information
- Uses environment variables SHELL (Unix) and COMSPEC (Windows) to determine the user's preferred shell
- Falls back to DEFAULT_SHELL if no shell is specified in environment variables
- Cross-platform implementation with different quoting strategies for Unix vs Windows
- Returns false for system() return codes 127 (command not found) or -1 (execution failed)
- Flushes all output streams before executing commands to ensure proper synchronization
- Part of psql's backslash command infrastructure for shell integration