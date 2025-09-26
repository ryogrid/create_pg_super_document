# InitializeLatchSupport

## Location
[src/backend/storage/ipc/latch.c:232-345](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/latch.c#L232-L345)

## Overview
Initializes the process-local latch infrastructure required for efficient waiting and signaling mechanisms in PostgreSQL processes.

## Definition
```c
void
InitializeLatchSupport(void)
```

## Detailed Description
This function sets up the platform-specific infrastructure needed for latch operations in PostgreSQL. It must be called once during startup of any process that needs to wait on latches, before any InitLatch() or OwnLatch() calls are made. The function handles different wait mechanisms based on compile-time configuration:

**WAIT_USE_SELF_PIPE Implementation:**
- Creates a self-pipe for signal-safe communication between signal handlers and waiting code
- Handles inheritance cleanup when running under postmaster (child processes close inherited pipes)
- Sets both pipe ends to non-blocking mode to prevent deadlocks
- Sets FD_CLOEXEC on both ends to prevent inheritance by child processes
- Registers SIGURG handler for latch signaling
- Tracks pipe ownership by process ID for proper cleanup

**WAIT_USE_SIGNALFD Implementation:**
- Sets up signalfd for receiving SIGURG notifications
- Blocks SIGURG signal since it's handled through signalfd
- Creates signalfd with non-blocking and close-on-exec flags

**WAIT_USE_KQUEUE Implementation:**
- Ignores SIGURG signal since kqueue will handle events

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - pipe (system call)
  - fcntl (system call)
  - close (system call)
  - elog
  - [ReleaseExternalFD](../R/ReleaseExternalFD.md)
  - [ReserveExternalFD](../R/ReserveExternalFD.md)
  - [pqsignal](../p/pqsignal.md)
  - [latch_sigurg_handler](../l/latch_sigurg_handler.md)
  - signalfd (system call, Linux)
  - sigaddset
  - sigemptyset
- Called from (representative examples):
  - [PostmasterMain](../P/PostmasterMain.md)
  - [InitPostmasterChild](InitPostmasterChild.md)
  - [InitStandaloneProcess](InitStandaloneProcess.md)

## Notes and Other Information
- Located in src/backend/storage/ipc/latch.c:232-345
- Must be called exactly once per process before any latch operations
- Handles platform-specific differences through compile-time conditionals
- Critical for PostgreSQL's efficient wait/notify mechanism
- Manages file descriptor accounting through fd.c integration
- Ensures proper cleanup of inherited resources in child processes
- Uses self-pipe trick on systems without native event notification (signalfd/kqueue)
- Sets up signal handling infrastructure for SIGURG-based latch signaling