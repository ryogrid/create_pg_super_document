# postmaster_is_alive

## Location
src/bin/pg_ctl/pg_ctl.c: 1312 - 1335

## Overview
A utility function that checks whether a PostgreSQL postmaster process with the given PID is still alive and running.

## Definition


## Detailed Description
This function performs a basic liveness check for a postmaster process by using the kill() system call with signal 0. The function implements several safety checks to avoid false positives:

1. **PID Validation**: Rejects the current process's own PID to prevent self-identification
2. **Parent Process Check**: On Unix systems, rejects the parent shell's PID to avoid confusion
3. **Process Existence Test**: Uses  to test if the process exists without sending an actual signal
4. **Permission Handling**: Treats EPERM errors as indication that the PID belongs to a different user, meaning it's not the target postmaster

The function is designed specifically for pg_ctl's needs to verify postmaster status, considering PostgreSQL's typical deployment scenarios and security model.

## Parameters / Member Variables
- : The process ID to check for liveness

## Dependencies
- Functions called/Symbols referenced:
  - getpid
  - getppid (Unix only)
  - kill
- Called from (representative examples):
  - do_restart (src/bin/pg_ctl/pg_ctl.c:1091, 1101)
  - do_status (src/bin/pg_ctl/pg_ctl.c:1348, 1358)

## Notes and Other Information
- The function is static, meaning it's only used within pg_ctl.c
- On Windows systems, the parent PID check is skipped since getppid() is not available
- EPERM errors are specifically handled to avoid false positives when checking PIDs belonging to other users
- This is a core utility function for pg_ctl's process management capabilities
- The function uses signal 0 with kill(), which is a standard Unix technique to test process existence without affecting the target process