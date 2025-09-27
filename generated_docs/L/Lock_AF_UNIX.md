# Lock_AF_UNIX

## Location
[src/backend/libpq/pqcomm.c:684-718](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L684-L718)

## Overview
Secures exclusive access to Unix domain socket paths by creating an interlock file, removing any existing socket file, and tracking the socket path for maintenance.

## Definition

```c
struct group *gr;
```
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
  - [CreateSocketLockFile](../C/CreateSocketLockFile.md)
  - unlink
  - [lappend](../l/lappend.md)
  - [pstrdup](../p/pstrdup.md)
  - STATUS_OK
- Called from (representative examples):
  - [ListenServerPort](ListenServerPort.md)

## Notes and Other Information
- This is a static function, only accessible within pqcomm.c
- Abstract sockets (Linux-specific, starting with '@') bypass the locking mechanism
- The lock file approach is more portable than direct socket file locking
- Socket paths are tracked in a global list (sock_paths) for cleanup during shutdown
- Prevents the common "address already in use" error when restarting PostgreSQL
- Race-condition free design ensures safe socket file management in multi-process environments

## Simplified Source

```c
// Simplified version of Lock_AF_UNIX
static int Lock_AF_UNIX(const char *unixSocketDir, const char *unixSocketPath) {
    // Skip locking for abstract sockets (Linux-specific feature)
    if (unixSocketPath[0] == '@') {
        return STATUS_OK;
    }

    // Create lock file to prevent other processes from using this socket path
    CreateSocketLockFile(unixSocketPath, true, unixSocketDir);

    // Remove any existing socket file to avoid bind() conflicts
    unlink(unixSocketPath);

    // Track socket path for cleanup during shutdown
    sock_paths = lappend(sock_paths, pstrdup(unixSocketPath));

    return STATUS_OK;
}
```

Key simplifications made:
- Removed detailed multi-line comments for clarity
- Consolidated the core logic into clear steps with brief descriptions
- Focused on the main execution path
- Preserved all essential functionality and error handling
- Maintained the original algorithm and logic flow