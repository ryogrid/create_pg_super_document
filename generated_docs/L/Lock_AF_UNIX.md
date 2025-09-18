# Lock_AF_UNIX

## Location
src/backend/libpq/pqcomm.c: 684 - 718

## Overview
Secures exclusive access to Unix domain socket paths by creating an interlock file, removing any existing socket file, and tracking the socket path for maintenance.

## Definition


## Detailed Description
The  function implements a file-based locking mechanism to ensure exclusive access to Unix domain socket paths. This prevents multiple PostgreSQL postmaster processes from attempting to bind to the same Unix socket path simultaneously.

Key operations performed:
- Checks for abstract sockets (paths starting with '@') which don't require file-based locking
- Creates a socket lock file using  to establish an interlock
- Safely removes any pre-existing socket file to avoid bind() failures
- Maintains a list of socket paths for cleanup during shutdown

The function uses a two-stage approach: first acquiring an exclusive lock file, then safely cleaning up any leftover socket files. This design is both portable across Unix systems and race-condition free, as the lock file prevents other processes from interfering during socket file cleanup and creation.

## Parameters / Member Variables
- : Directory where the Unix domain socket will be created
- : Full path to the Unix domain socket file

## Dependencies
- Functions called/Symbols referenced:
  - CreateSocketLockFile
  - unlink
  - lappend
  - pstrdup
  - STATUS_OK
- Called from (representative examples):
  - ListenServerPort

## Notes and Other Information
- This is a static function, only accessible within pqcomm.c
- Abstract sockets (Linux-specific, starting with '@') bypass the locking mechanism
- The lock file approach is more portable than direct socket file locking
- Socket paths are tracked in a global list (sock_paths) for cleanup during shutdown
- Prevents the common "address already in use" error when restarting PostgreSQL
- Race-condition free design ensures safe socket file management in multi-process environments