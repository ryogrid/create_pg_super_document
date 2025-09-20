# pg_signal_backend

## Location
[src/backend/storage/ipc/signalfuncs.c:49-121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/signalfuncs.c#L49-L121)

## Overview
Internal function that provides the core mechanism for sending Unix signals to PostgreSQL backend processes with proper permission checking and validation.

## Definition

```c
static int
pg_signal_backend(int pid, int sig)
```
## Detailed Description
pg_signal_backend is a static helper function that implements the core logic for safely sending signals to PostgreSQL backend processes. It performs comprehensive validation including:

1. **Process Validation**: Uses BackendPidGetProc() to verify the PID corresponds to a valid PostgreSQL backend process
2. **Permission Checking**: Enforces role-based access control, ensuring users can only signal processes they have appropriate privileges for
3. **Superuser Protection**: Prevents non-superusers from signaling superuser-owned backends
4. **Safe Signal Delivery**: Uses kill() system call with process group signaling when available (HAVE_SETSID)

The function returns status codes to indicate success or the specific type of failure encountered, allowing callers to handle different error conditions appropriately.

## Parameters / Member Variables
- `pid`: Process ID of the target PostgreSQL backend process to signal
- `sig`: Unix signal number to send to the target process (e.g., SIGTERM, SIGINT)

## Dependencies
- Functions called/Symbols referenced:
  - BackendPidGetProc
  - superuser_arg
  - superuser
  - has_privs_of_role
  - [GetUserId](../G/GetUserId.md)
  - kill (system call)
- Called from (representative examples):
  - [pg_cancel_backend](pg_cancel_backend.md)
  - [pg_terminate_backend](pg_terminate_backend.md)

## Notes and Other Information
- This is a static function, only accessible within signalfuncs.c
- Returns SIGNAL_BACKEND_SUCCESS on success, or error codes like SIGNAL_BACKEND_ERROR, SIGNAL_BACKEND_NOSUPERUSER, SIGNAL_BACKEND_NOPERMISSION
- When HAVE_SETSID is defined, signals the entire process group using -pid instead of just the specific process
- Includes race condition commentary noting the extremely low probability of PID recycling issues
- Uses WARNING level logging for non-fatal errors to allow batch operations to continue
- Cannot be used to signal auxiliary processes or the postmaster - only regular backend processes