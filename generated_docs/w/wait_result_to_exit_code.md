# wait_result_to_exit_code

## Location
[src/common/wait_error.c:138-148](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wait_error.c#L138-L148)

## Overview
Converts a wait status value into a shell-compatible exit code, normalizing different termination scenarios into the standard 0-255 exit code range.

## Definition
```c
int wait_result_to_exit_code(int exit_status)
```

## Detailed Description
This utility function translates complex wait status values into conventional shell exit codes, providing a standardized interface for process result handling. It implements the POSIX shell convention for representing various process termination scenarios as simple integer exit codes.

The function handles three primary cases:
1. Error conditions (exit_status == -1): Passes through -1 unchanged, typically indicating pclose() or system() failures
2. Normal process termination: Extracts and returns the actual exit code using WEXITSTATUS
3. Signal termination: Converts to shell convention of 128 + signal number using WTERMSIG

This conversion is essential for maintaining compatibility with shell scripting conventions and providing consistent exit code semantics across different process execution methods.

## Parameters / Member Variables
- `exit_status`: The integer status value returned by wait(), waitpid(), pclose(), or system() system calls

## Dependencies
- Functions called/Symbols referenced:
  - WIFEXITED (macro to test if process exited normally)
  - WEXITSTATUS (macro to extract exit status code)
  - WIFSIGNALED (macro to test if process was terminated by signal)
  - WTERMSIG (macro to extract terminating signal number)

- Called from (representative examples):
  - SetShellResultVariables

## Notes and Other Information
- Returns exit codes in the conventional shell range of 0-255 for normal operation
- Special handling for -1 return values from pclose() and system() calls
- Implements POSIX convention of 128 + signal number for signal termination
- The function includes a fallback return of -1 for unrecognized status values, though this is typically unreachable on most systems
- Part of PostgreSQL's common utility library for process management and shell integration
- Primarily used in psql for setting shell result variables that scripts can access