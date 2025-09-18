# Latch

## Location
src/include/storage/latch.h: 112 - 121

## Overview
Latch is a lightweight synchronization primitive in PostgreSQL that provides efficient inter-process signaling and waiting mechanisms, designed for minimal overhead and safe use in signal handlers and critical sections.

## Definition


## Detailed Description
The Latch structure serves as PostgreSQL's primary mechanism for efficient process synchronization and signaling. It is designed to be a lightweight alternative to more heavyweight synchronization primitives like semaphores or condition variables. Latches can be used both within a single process and across multiple processes, making them versatile for various PostgreSQL subsystems including WAL processing, background workers, and client-server communication.

The latch mechanism is designed with several key principles:
- **Signal safety**: Can be safely used in signal handlers
- **Low overhead**: Minimal cost when already set
- **Cross-platform**: Supports both Unix-like systems and Windows
- **Memory barrier aware**: Ensures proper memory ordering for shared data

Latches support multiple wait conditions including timeout, postmaster death detection, and socket readiness, making them suitable for complex event-driven scenarios.

## Parameters / Member Variables
- : Atomic flag indicating whether the latch has been set/signaled
- : Atomic flag indicating if a process might be sleeping on this latch, used for optimization
- : Boolean flag indicating whether this is a shared latch (cross-process) or process-local
- : Process ID of the process that owns/can wait on this latch
-  (Windows only): Windows event handle used for cross-process signaling

## Dependencies
- Functions called/Symbols referenced:
  - pg_memory_barrier (for memory ordering)
  - MyProcPid (current process ID)
  - CreateEvent (Windows API)
  - SetEvent (Windows API)
  - kill (Unix signal sending)

- Called from (representative examples):
  - [WaitEventSet](../W/WaitEventSet.md) (event waiting infrastructure)
  - [XLogRecoveryCtlData](../X/XLogRecoveryCtlData.md) (WAL recovery control)
  - WalRcvState (WAL receiver state)
  - [WalSnd](../W/WalSnd.md) (WAL sender processes)
  - [PGPROC](../P/PGPROC.md) (process control blocks)

## Key Functions
- : Initialize a process-local latch
- : Initialize a shared latch for cross-process use
- : Signal/set a latch to wake up waiters
- : Clear a latch's set state
- : Wait for latch to be set with optional timeout and conditions
- : Take ownership of a shared latch
- : Release ownership of a shared latch

## Notes and Other Information
- Latches should be treated as opaque structures; only access through public functions
- The structure is designed to be embeddable in larger structures for efficiency
- On Unix systems, uses SIGURG signals or self-pipe trick for notification
- On Windows, uses Win32 Event objects for synchronization
- Memory barriers ensure proper ordering when checking/setting flags
- Safe to call SetLatch() from signal handlers and critical sections
- Supports integration with PostgreSQL's wait event system for monitoring and timeouts