# OwnLatch

## Location
[src/backend/storage/ipc/latch.c:463-488](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/latch.c#L463-L488)

## Overview
Associates a shared latch with the current process, giving the process ownership and the ability to wait on the latch.

## Definition
```c
void OwnLatch(Latch *latch)
```

## Detailed Description
OwnLatch takes ownership of a shared latch by setting the latch's owner_pid to the current process ID. This function is used after a shared latch has been initialized with InitSharedLatch to associate it with a specific process that needs to wait on it. The function includes sanity checks to ensure the latch is marked as shared and that no other process currently owns it. On Unix-like systems, it also verifies that the necessary latch support mechanisms have been properly initialized in the current process.

## Parameters / Member Variables
- `latch`: Pointer to the shared Latch structure to take ownership of

## Dependencies
- Functions called/Symbols referenced:
  - [Latch](../L/Latch.md) (structure type)
  - WAIT_USE_SELF_PIPE (conditional compilation flag)
  - WAIT_USE_SIGNALFD (conditional compilation flag)
  - PANIC (error level)
- Called from (representative examples):
  - [InitWalRecovery](../I/InitWalRecovery.md)
  - [InitProcess](../I/InitProcess.md)
  - [InitAuxiliaryProcess](../I/InitAuxiliaryProcess.md)

## Notes and Other Information
- Only works with shared latches (latch->is_shared must be true)
- Panics if the latch is already owned by another process
- No locking is performed, so callers must provide interlocks if concurrent ownership attempts are possible
- Requires InitializeLatchSupport to have been called in the current process on Unix systems
- Once owned, the process can use WaitLatch and related functions on this latch

## Simplified Source

```c
// Simplified version of OwnLatch
void OwnLatch(Latch *latch) {
    int owner_pid;

    // Verify this is a shared latch
    Assert(latch->is_shared);

    // Platform-specific checks for latch support initialization
    // (Self-pipe or signalfd mechanisms must be ready)
    platform_specific_latch_checks();

    // Check if latch is already owned
    owner_pid = latch->owner_pid;
    if (owner_pid != 0) {
        elog(PANIC, "latch already owned by PID %d", owner_pid);
    }

    // Take ownership by setting our process ID
    latch->owner_pid = MyProcPid;
}
```

Key simplifications made:
- Consolidated platform-specific conditional compilation checks into a single conceptual step
- Removed detailed macro definitions (WAIT_USE_SELF_PIPE, WAIT_USE_SIGNALFD) for clarity
- Abstracted low-level file descriptor checks while preserving the essential validation logic
- Focused on the main execution path: validate → check ownership → claim ownership
- Maintained the critical error handling for already-owned latches