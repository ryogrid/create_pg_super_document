# wait_result_is_signal

## Location
[src/common/wait_error.c:102-120](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wait_error.c#L102-L120)

## Overview
Determines if a child process terminated due to a specific signal, handling both direct signal termination and shell-mediated signal reporting.

## Definition
```c
bool wait_result_is_signal(int exit_status, int signum)
```

## Detailed Description
This utility function provides a robust method for detecting signal-based process termination, accounting for the complexity of Unix process hierarchies. It addresses a common scenario where a shell process may be intermediary between the parent and child processes.

The function handles two distinct signal reporting mechanisms:
1. Direct signal termination: The child process received the signal directly and the wait status reflects this via WIFSIGNALED/WTERMSIG macros
2. Shell-mediated termination: A shell process was intermediate and reports the child's signal death using the POSIX convention of exit code 128 + signal number

This dual-mode detection is essential for reliable signal handling in environments where shell command execution is involved.

## Parameters / Member Variables
- `exit_status`: The integer status value returned by wait(), waitpid(), or similar system calls
- `signum`: The specific signal number to test for (e.g., SIGTERM, SIGINT)

## Dependencies
- Functions called/Symbols referenced:
  - WIFSIGNALED (macro to test if process was terminated by signal)
  - WTERMSIG (macro to extract terminating signal number)
  - WIFEXITED (macro to test if process exited normally)
  - WEXITSTATUS (macro to extract exit status code)

- Called from (representative examples):
  - RestoreArchivedFile
  - ClosePipeFromProgram

## Notes and Other Information
- The function is specifically designed for scenarios involving potential shell intermediaries
- Implements POSIX standard shell behavior for signal reporting (128 + signal number)
- Should not be used when there is no possibility of an intermediate shell process
- Returns boolean true if the specified signal caused the process termination, false otherwise
- Part of PostgreSQL's common utility library for cross-component signal handling