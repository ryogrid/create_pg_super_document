# signal_cleanup

## Location
[src/bin/pg_test_fsync/pg_test_fsync.c:599-614](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_test_fsync/pg_test_fsync.c#L599-L614)

## Overview
A signal handler function in pg_test_fsync that performs cleanup operations when the program receives a signal, ensuring proper resource cleanup and termination.

## Definition

```c
static void
signal_cleanup(SIGNAL_ARGS)
```
## Detailed Description
The signal_cleanup function serves as a signal handler for pg_test_fsync utility. When invoked due to a signal (typically SIGINT or SIGTERM), it performs essential cleanup operations before terminating the process. The function ensures that any temporary files created during testing are properly removed and that stdout output is properly terminated with a newline. After cleanup, it exits the process with status code 1.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - unlink (system call for file deletion)
  - write (system call for output)
  - _exit (system call for process termination)
  - STDOUT_FILENO (standard output file descriptor)
- Called from (representative examples):
  - STOP_TIMER macro (multiple locations)
  - [main](../m/main.md) function (signal handler registration)

## Notes and Other Information
- Uses global variables  and  to determine cleanup actions
- Performs error-tolerant cleanup by ignoring unlink errors
- Uses  instead of  for immediate termination without cleanup handlers
- Silences compiler warnings about unused return value from write()
- Critical for proper cleanup when pg_test_fsync is interrupted during testing