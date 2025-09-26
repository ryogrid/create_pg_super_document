# PostmasterIsAliveInternal

## Location
src/backend/storage/ipc/pmsignal.c: 376 - 436

## Overview
Checks whether the postmaster process is still alive using platform-specific mechanisms, serving as the slow path for the PostmasterIsAlive() function.

## Definition


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
  - WaitEventSetWaitBlock (src/backend/storage/ipc/latch.c:1645)
  - PostmasterIsAlive (src/include/storage/pmsignal.h:99, 102)

## Notes and Other Information
- This is the "slow path" for postmaster liveness detection - the fast path checks a signal flag first
- On platforms without postmaster death signal support, PostmasterIsAlive() is just an alias for this function
- The function includes careful error handling and will call elog(FATAL) for unexpected conditions
- Critical for proper cleanup and error handling when the postmaster process terminates unexpectedly
- Uses non-blocking I/O operations to avoid hanging the calling process