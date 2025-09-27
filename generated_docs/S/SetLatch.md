# SetLatch

## Location
[src/backend/storage/ipc/latch.c:632-723](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/latch.c#L632-L723)

## Overview
SetLatch signals a latch to wake up any processes or threads waiting on it, providing a thread-safe mechanism for inter-process communication and coordination.

## Definition

```c
void
SetLatch(Latch *latch)
```
## Detailed Description
SetLatch is a critical synchronization primitive that sets a latch's state to signaled and wakes up any processes waiting on it. The function uses memory barriers to ensure proper ordering of memory operations and includes platform-specific optimizations for both Unix-like systems and Windows.

The function performs a quick exit if the latch is already set to avoid unnecessary work. When signaling is needed, it uses different mechanisms depending on the platform: on Unix systems, it sends SIGURG signals or writes to a self-pipe; on Windows, it calls SetEvent on the associated event handle. The function is designed to be safe for use in signal handlers and critical sections, avoiding operations that might throw errors.

## Parameters / Member Variables
- : Pointer to the Latch structure to signal and set to the awakened state

## Dependencies
- Functions called/Symbols referenced:
  - pg_memory_barrier (memory synchronization)
  - [sendSelfPipeByte](../s/sendSelfPipeByte.md) (Unix self-pipe wakeup mechanism)
  - kill (Unix signal sending)
  - SetEvent (Windows event signaling)
  - MyProcPid (current process ID)
- Called from (representative examples):
  - [HandleParallelMessageInterrupt](../H/HandleParallelMessageInterrupt.md) (parallel processing)
  - [WakeupRecovery](../W/WakeupRecovery.md) (WAL recovery coordination)
  - [ReqCheckpointHandler](../R/ReqCheckpointHandler.md) (checkpoint coordination)
  - [ConditionVariableSignal](../C/ConditionVariableSignal.md) (condition variable implementation)
  - [ProcessClientReadInterrupt](../P/ProcessClientReadInterrupt.md) (client communication)

## Notes and Other Information
- The function is designed to be safe for use in signal handlers and critical sections
- Uses memory barriers to ensure proper ordering of memory operations before and after setting the latch state
- Includes platform-specific implementations for Unix (using signals/self-pipe) and Windows (using events)
- Has built-in optimizations to avoid unnecessary work when the latch is already set
- Race conditions are carefully managed through atomic operations and proper memory barriers
- Error handling is minimal to maintain safety in critical code paths
- The function assumes that pid_t is atomic, which may not be strictly guaranteed but works in practice

## Simplified Source

```c
// Simplified version of SetLatch
void SetLatch(Latch *latch) {
    // Ensure memory ordering for any flag variables changed by this process
    pg_memory_barrier();

    // Quick exit if latch is already set
    if (latch->is_set)
        return;

    // Set the latch state
    latch->is_set = true;

    // Check if anyone is waiting
    pg_memory_barrier();
    if (!latch->maybe_sleeping)
        return;

#ifndef WIN32
    // Unix/Linux: Wake up waiting processes using signals
    pid_t owner_pid = latch->owner_pid;

    if (owner_pid == 0) {
        // No owner, nothing to wake up
        return;
    } else if (owner_pid == MyProcPid) {
        // Wake up ourselves (signal handler case)
        if (waiting) {
            // Use self-pipe or SIGURG to wake up current process
            sendSelfPipeByte_or_kill_self();
        }
    } else {
        // Wake up another process
        kill(owner_pid, SIGURG);
    }
#else
    // Windows: Wake up waiting processes using events
    HANDLE handle = latch->event;
    if (handle) {
        SetEvent(handle);  // Ignore any errors for safety
    }
#endif
}
```

Key simplifications made:
- Removed detailed comments and explanations for clarity
- Abstracted the self-pipe vs SIGURG choice into a single conceptual operation
- Consolidated the signal handling logic into clearer branches
- Focused on the main execution flow
- Simplified error handling strategy to just "ignore errors for safety"
- Removed detailed race condition explanations while preserving the essential logic