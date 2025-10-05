# ShutdownLatchSupport

## Location
[src/backend/storage/ipc/latch.c:364-393](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/latch.c#L364-L393)

## Overview
Cleans up and shuts down the process-local latch infrastructure, releasing all associated resources and file descriptors.

## Definition
```c
void
ShutdownLatchSupport(void)
```

## Detailed Description
This function performs cleanup of all latch-related infrastructure that was set up by InitializeLatchSupport(). It handles platform-specific cleanup based on compile-time configuration and ensures proper resource deallocation. The function performs the following cleanup operations:

**WAIT_USE_POLL Implementation:**
- Resets SIGURG signal handler to SIG_IGN

**Global LatchWaitSet Cleanup:**
- Frees the global LatchWaitSet if it exists using FreeWaitEventSet()
- Sets LatchWaitSet to NULL

**WAIT_USE_SELF_PIPE Implementation:**
- Closes both ends of the self-pipe (read and write file descriptors)
- Resets file descriptor variables to -1
- Resets owner PID to InvalidPid

**WAIT_USE_SIGNALFD Implementation:**
- Closes the signalfd file descriptor
- Resets signal_fd to -1

The function ensures that all resources allocated during latch initialization are properly released, preventing resource leaks during process shutdown.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [pqsignal](../p/pqsignal.md)
  - [FreeWaitEventSet](../F/FreeWaitEventSet.md)
  - close (system call)
  - LatchWaitSet (global variable)
  - SIG_IGN (signal constant)
  - InvalidPid (constant)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/storage/ipc/latch.c:364-393
- Designed to be called during process shutdown or cleanup
- Handles platform-specific differences through compile-time conditionals
- Ensures proper cleanup of file descriptors to prevent resource leaks
- Currently appears to have no direct callers in the codebase, suggesting it may be used in cleanup paths or error handling
- Complements InitializeLatchSupport() by undoing all its initialization work
- Critical for proper resource management in long-running PostgreSQL processes

## Simplified Source

```c
void ShutdownLatchSupport(void) {
    // Reset signal handler for poll-based implementation
    #if defined(WAIT_USE_POLL)
        pqsignal(SIGURG, SIG_IGN);
    #endif

    // Clean up the global wait event set
    if (LatchWaitSet) {
        FreeWaitEventSet(LatchWaitSet);
        LatchWaitSet = NULL;
    }

    // Close self-pipe file descriptors and reset state
    #if defined(WAIT_USE_SELF_PIPE)
        close(selfpipe_readfd);
        close(selfpipe_writefd);
        selfpipe_readfd = -1;
        selfpipe_writefd = -1;
        selfpipe_owner_pid = InvalidPid;
    #endif

    // Close signal file descriptor
    #if defined(WAIT_USE_SIGNALFD)
        close(signal_fd);
        signal_fd = -1;
    #endif
}
```