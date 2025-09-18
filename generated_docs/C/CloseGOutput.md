# CloseGOutput

## Location
src/bin/psql/common.c: 110 - 131

## Overview
Closes the output stream opened for psql's \g command, handling both regular files and pipes with appropriate cleanup and signal restoration.

## Definition
```c
static void CloseGOutput(FILE *gfile_fout, bool is_pipe)
```

## Detailed Description
This static function provides proper cleanup for output streams opened by SetupGOutput(). It handles two distinct cases based on the output type:

1. **Pipe cleanup**: Uses pclose() to properly terminate the pipe process, captures the exit status via SetShellResultVariables(), and restores the original SIGPIPE signal handler
2. **Regular file cleanup**: Uses fclose() to close the file handle

The function ensures that all resources are properly released and that signal handling is restored to its original state after pipe operations. This is crucial for maintaining consistent behavior and preventing resource leaks.

## Parameters / Member Variables
- `gfile_fout`: FILE* handle to the output stream that needs to be closed (can be NULL)
- `is_pipe`: Boolean flag indicating whether the output is a pipe (affects cleanup method)

## Dependencies
- Functions called/Symbols referenced:
  - [SetShellResultVariables](../S/SetShellResultVariables.md) (to capture pipe command exit status)
  - [pclose](../p/pclose.md) (to close pipe and wait for process termination)
  - [restore_sigpipe_trap](../r/restore_sigpipe_trap.md) (to restore original SIGPIPE handling)
  - fclose (to close regular files)
- Called from (representative examples):
  - [ExecQueryAndProcessResults](../E/ExecQueryAndProcessResults.md)

## Notes and Other Information
- This is a static function, only accessible within common.c
- The function safely handles NULL file pointers by checking before attempting to close
- For pipes, the exit status of the command is captured and made available through shell result variables
- Signal handling restoration is automatic for pipe operations
- Works as a companion to SetupGOutput() for complete resource management
- No return value as cleanup operations are expected to succeed