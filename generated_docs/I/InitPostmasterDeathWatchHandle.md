# InitPostmasterDeathWatchHandle

## Location
[src/backend/postmaster/postmaster.c:4705-4752](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/postmaster.c#L4705-L4752)

## Overview
Initializes a cross-platform mechanism for child processes to monitor whether the postmaster process is still alive, using pipes on Unix-like systems and process handles on Windows.

## Definition
```c
static void InitPostmasterDeathWatchHandle(void)
```

## Detailed Description
This function establishes a postmaster death monitoring mechanism that allows child processes to detect when the postmaster (parent) process terminates. The implementation differs between platforms:

**On Unix-like systems (non-WIN32):**
- Creates a pipe using `pipe(postmaster_alive_fds)`
- The postmaster holds the write end (`POSTMASTER_FD_OWN`) open
- Child processes hold the read end (`POSTMASTER_FD_WATCH`) 
- When the postmaster dies, the pipe becomes readable (EOF condition)
- Sets the read end to non-blocking mode for polling-style checks
- Reserves file descriptors with the fd.c subsystem

**On Windows (WIN32):**
- Duplicates the current process handle using `DuplicateHandle`
- Stores the duplicated handle in the global `PostmasterHandle` variable
- Child processes can use `WaitForSingleObject` on this handle to detect termination
- The handle is inheritable (TRUE parameter) so child processes receive a copy

This mechanism is essential for PostgreSQL's process management, allowing background workers and other child processes to gracefully shut down when the postmaster terminates unexpectedly.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - `pipe` - Unix system call to create pipe (non-Windows)
  - `fcntl` - Set file descriptor flags (non-Windows) 
  - `[ReserveExternalFD](../R/ReserveExternalFD.md)` - PostgreSQL FD tracking (non-Windows)
  - `DuplicateHandle` - Windows API to duplicate process handle (Windows)
  - `GetCurrentProcess` - Windows API to get current process handle (Windows)
  - `ereport` - PostgreSQL error reporting
  - [errcode_for_file_access](../e/errcode_for_file_access.md), `errcode_for_socket_access` - PostgreSQL error codes

- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md) - During postmaster initialization
  - `SignalChildren` - During shutdown process

## Notes and Other Information
- This function must only be called once in the postmaster process, as asserted by `Assert(MyProcPid == PostmasterPid)`
- The monitoring mechanism is bidirectional: children can detect postmaster death, and the postmaster can clean up by closing handles
- Child processes must close the write end of the pipe (`POSTMASTER_FD_OWN`) in `ClosePostmasterPorts()` to ensure EOF is properly signaled
- On Windows, the process handle allows for both polling (with timeout 0) and blocking waits
- Failure to initialize this mechanism is considered FATAL as it's critical for proper process lifecycle management
- The non-blocking flag on Unix allows children to test for postmaster death without blocking via `read()` calls that return 0 on EOF

## Simplified Source

```c
// Simplified version of InitPostmasterDeathWatchHandle
static void InitPostmasterDeathWatchHandle(void) {
    // Verify this is called only in the postmaster process
    Assert(MyProcPid == PostmasterPid);

#ifndef WIN32
    // Unix/Linux: Create a pipe for death monitoring
    // Parent keeps write end, children get read end
    if (pipe(postmaster_alive_fds) < 0) {
        ereport(FATAL, (errmsg_internal("could not create pipe to monitor postmaster death")));
    }

    // Reserve file descriptors for the pipe
    ReserveExternalFD();
    ReserveExternalFD();

    // Set read end to non-blocking for polling
    if (fcntl(postmaster_alive_fds[POSTMASTER_FD_WATCH], F_SETFL, O_NONBLOCK) == -1) {
        ereport(FATAL, (errmsg_internal("could not set pipe to nonblocking mode")));
    }

#else
    // Windows: Duplicate process handle for death monitoring
    if (DuplicateHandle(GetCurrentProcess(), GetCurrentProcess(),
                       GetCurrentProcess(), &PostmasterHandle,
                       0, TRUE, DUPLICATE_SAME_ACCESS) == 0) {
        ereport(FATAL, (errmsg_internal("could not duplicate postmaster handle")));
    }
#endif
}
```

Key simplifications made:
- Removed detailed error codes for clarity
- Simplified error messages while preserving essential information
- Consolidated comments to focus on core functionality
- Maintained the critical platform-specific logic paths
- Preserved all essential error handling and assertions