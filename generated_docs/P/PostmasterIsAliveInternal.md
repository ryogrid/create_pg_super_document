# PostmasterIsAliveInternal

## Location
[src/backend/storage/ipc/pmsignal.c:376-436](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/pmsignal.c#L376-L436)

## Overview
Checks whether the postmaster process is still alive using platform-specific mechanisms, serving as the slow path for the PostmasterIsAlive() function.

## Definition

```c
bool
PostmasterIsAliveInternal(void)
```
## Detailed Description
This function performs the actual work of checking if the postmaster process is still running. It implements platform-specific logic to detect postmaster death:

On Unix-like systems, it uses a pipe-based monitoring mechanism where it attempts to read from a file descriptor (postmaster_alive_fds[POSTMASTER_FD_WATCH]). If the read would block (EAGAIN/EWOULDBLOCK), the postmaster is still alive. If data is available or an error occurs, the postmaster has died.

On Windows, it uses WaitForSingleObject() on the PostmasterHandle to check if the process is still running.

On platforms supporting USE_POSTMASTER_DEATH_SIGNAL, this function works in conjunction with a signal-based fast path. It resets the postmaster_possibly_dead flag before checking and sets it again if death is detected.

This is typically called through the PostmasterIsAlive() macro/inline function, which first checks the fast path signal flag before calling this slower detection method.

## Parameters / Member Variables
This function takes no parameters but accesses:
- : Global flag indicating potential postmaster death (on supporting platforms)
- : Array of file descriptors for postmaster monitoring (Unix)
- : Windows handle to the postmaster process (Windows)

## Dependencies
- Platform-specific APIs:
  - read() (Unix)
  - WaitForSingleObject() (Windows)
- Constants used:
  - POSTMASTER_FD_WATCH
  - EAGAIN, EWOULDBLOCK (Unix)
  - WAIT_TIMEOUT (Windows)
- Conditional compilation:
  - USE_POSTMASTER_DEATH_SIGNAL
  - WIN32
- Called from:
  - [WaitEventSetWaitBlock](../W/WaitEventSetWaitBlock.md) (src/backend/storage/ipc/latch.c:1645)
  - [PostmasterIsAlive](PostmasterIsAlive.md) (src/include/storage/pmsignal.h:99, 102)

## Notes and Other Information
- This is the "slow path" for postmaster liveness detection - the fast path checks a signal flag first
- On platforms without postmaster death signal support, PostmasterIsAlive() is just an alias for this function
- The function includes careful error handling and will call elog(FATAL) for unexpected conditions
- Critical for proper cleanup and error handling when the postmaster process terminates unexpectedly
- Uses non-blocking I/O operations to avoid hanging the calling process

## Simplified Source

```c
// Simplified version of PostmasterIsAliveInternal
bool PostmasterIsAliveInternal(void) {
#ifdef USE_POSTMASTER_DEATH_SIGNAL
    // Reset the death flag before checking
    postmaster_possibly_dead = false;
#endif

#ifndef WIN32
    // Unix/Linux: Check via pipe read
    char c;
    ssize_t rc = read(postmaster_alive_fds[POSTMASTER_FD_WATCH], &c, 1);

    // If read would block, postmaster is alive
    if (rc < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        return true;
    }

    // If we get here, postmaster is dead or error occurred
#ifdef USE_POSTMASTER_DEATH_SIGNAL
    postmaster_possibly_dead = true;
#endif

    // Handle unexpected conditions with fatal errors
    if (rc < 0) {
        elog(FATAL, "read on postmaster death monitoring pipe failed: %m");
    } else if (rc > 0) {
        elog(FATAL, "unexpected data in postmaster death monitoring pipe");
    }

    return false;

#else  // WIN32
    // Windows: Check process handle status
    if (WaitForSingleObject(PostmasterHandle, 0) == WAIT_TIMEOUT) {
        return true;  // Still running
    } else {
#ifdef USE_POSTMASTER_DEATH_SIGNAL
        postmaster_possibly_dead = true;
#endif
        return false;  // Process terminated
    }
#endif
}
```

Key simplifications made:
- Consolidated platform-specific logic into clear sections
- Added inline comments explaining the core detection mechanism
- Simplified the control flow while preserving all error handling
- Maintained the essential pipe-based detection on Unix and handle-based detection on Windows