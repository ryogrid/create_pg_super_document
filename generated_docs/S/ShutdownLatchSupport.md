# ShutdownLatchSupport

## Location
src/backend/storage/ipc/latch.c: 364 - 393

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
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - pqsignal
  - FreeWaitEventSet
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