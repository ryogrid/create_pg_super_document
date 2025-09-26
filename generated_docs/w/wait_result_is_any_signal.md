# wait_result_is_any_signal

## Location
[src/common/wait_error.c:121-137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wait_error.c#L121-L137)

## Overview
Determines if a child process terminated due to any signal or abnormal condition, with optional inclusion of command execution errors.

## Definition
```c
bool wait_result_is_any_signal(int exit_status, bool include_command_not_found)
```

## Detailed Description
This utility function provides a comprehensive test for process termination by signals or abnormal conditions, offering flexibility in how command execution errors are handled. It serves as a broader companion to wait_result_is_signal() by detecting any signal-based termination rather than a specific signal.

The function operates on two detection mechanisms:
1. Direct signal termination via WIFSIGNALED macro
2. Shell-mediated signal reporting through exit codes greater than 128 (following POSIX convention of 128 + signal number)

Additionally, when include_command_not_found is true, the function treats shell exit codes 126 (command not executable) and 127 (command not found) as abnormal termination conditions, providing a unified interface for detecting both signal-based and command execution failures.

## Parameters / Member Variables
- `exit_status`: The integer status value returned by wait(), waitpid(), or similar system calls
- `include_command_not_found`: Boolean flag controlling whether to treat command execution errors (codes 126-127) as abnormal termination

## Dependencies
- Functions called/Symbols referenced:
  - WIFSIGNALED (macro to test if process was terminated by signal)
  - WIFEXITED (macro to test if process exited normally)
  - WEXITSTATUS (macro to extract exit status code)

- Called from (representative examples):
  - [RestoreArchivedFile](../R/RestoreArchivedFile.md)
  - [ExecuteRecoveryCommand](../E/ExecuteRecoveryCommand.md)
  - [shell_archive_file](../s/shell_archive_file.md)

## Notes and Other Information
- Uses threshold comparison (exit code > 125 or > 128) to detect shell-reported signal termination
- The include_command_not_found parameter provides flexibility for different error handling strategies
- When include_command_not_found is true, exit codes 126 and 127 are treated as abnormal conditions
- When false, only exit codes > 128 (signal-based) are considered abnormal
- Part of PostgreSQL's wait error handling utilities for robust process management
- Particularly useful in archive and recovery operations where external commands may fail