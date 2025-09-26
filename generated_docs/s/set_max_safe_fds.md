# set_max_safe_fds

## Location
[src/backend/storage/file/fd.c:1041-1083](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L1041-L1083)

## Overview
Determines and sets the maximum number of file descriptors that PostgreSQL's fd.c subsystem is allowed to use, ensuring safe operation within system limits while reserving descriptors for other purposes.

## Definition
```c
void set_max_safe_fds(void)
```

## Detailed Description
This function establishes a conservative limit on file descriptor usage by PostgreSQL to prevent exhausting the system's file descriptor resources. It performs the following calculation:

1. Uses `count_usable_fds()` to determine how many FDs are actually available
2. Calculates the safe limit as: MIN(usable_fds, max_files_per_process - already_open)
3. Subtracts NUM_RESERVED_FDS to account for system calls like `system()`
4. Ensures the result meets the minimum requirement (FD_MINFREE)

The function sets the global variable `max_safe_fds` which is used throughout PostgreSQL to limit file descriptor allocation. This prevents the server from hitting hard limits that would cause EMFILE errors and potential system instability.

## Parameters / Member Variables
- No parameters (void function)
- Sets global variable `max_safe_fds` as a side effect

## Dependencies
- Functions called/Symbols referenced:
  - count_usable_fds
  - Min (macro)
  - ereport
  - elog
  - Constants: NUM_RESERVED_FDS, FD_MINFREE, max_files_per_process
- Called from (representative examples):
  - PostmasterMain
  - Referenced in header file src/include/storage/fd.h

## Notes and Other Information
- Called during PostgreSQL startup to establish safe file descriptor limits
- The calculation ensures PostgreSQL never exhausts available file descriptors
- NUM_RESERVED_FDS accounts for descriptors needed by system calls and other non-PostgreSQL operations
- FD_MINFREE ensures PostgreSQL retains enough descriptors for basic operation
- Terminates the server with FATAL error if insufficient descriptors are available
- Logs the calculated limits at DEBUG2 level for diagnostics
- This is a critical initialization function that affects all subsequent file operations in PostgreSQL