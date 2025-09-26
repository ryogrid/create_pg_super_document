# wait_result_to_str

## Location
[src/common/wait_error.c:33-101](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wait_error.c#L33-L101)

## Overview
Converts a wait status value returned by wait(2), waitpid(2), pclose(3), or system(3) into a human-readable string explaining why a child process terminated.

## Definition
```c
char *wait_result_to_str(int exitstatus)
```

## Detailed Description
This utility function provides comprehensive interpretation of process exit status codes, handling various termination scenarios including normal exits, signals, and error conditions. The function translates numeric status codes into localized, descriptive messages that are more meaningful for logging and error reporting.

The function handles several distinct cases:
- Status -1 indicating a system call error (uses errno via %m format specifier)
- Normal process exit with special handling for common shell exit codes (126 for non-executable, 127 for command not found)
- Signal termination with platform-specific formatting (Windows exceptions vs Unix signals)
- Unrecognized status values as fallback

The returned string is allocated using pstrdup() and must be freed by the caller.

## Parameters / Member Variables
- `exitstatus`: The integer status value returned by wait(), waitpid(), pclose(), or system() system calls

## Dependencies
- Functions called/Symbols referenced:
  - WIFEXITED (macro to test if process exited normally)
  - WEXITSTATUS (macro to extract exit status)
  - WIFSIGNALED (macro to test if process was terminated by signal)
  - WTERMSIG (macro to extract terminating signal)
  - pg_strsignal (PostgreSQL function to get signal name)
  - pstrdup (PostgreSQL memory allocation wrapper)

- Called from (representative examples):
  - RestoreArchivedFile
  - ExecuteRecoveryCommand
  - ClosePipeFromProgram
  - BaseBackup
  - pclose_check

## Notes and Other Information
- The function is part of PostgreSQL's common utility library (src/common/)
- Provides platform-specific handling for Windows (exception codes) vs Unix-like systems (signal names)
- Uses PostgreSQL's internationalization framework with _() macro for translatable strings
- Special handling for common shell exit codes 126 and 127 provides more user-friendly error messages
- Memory management follows PostgreSQL conventions with pstrdup() allocation requiring caller to free result