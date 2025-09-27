# CheckDeadLockAlert

## Location
[src/backend/storage/lmgr/proc.c:1845-1870](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/proc.c#L1845-L1870)

## Overview
Handles the expiry of deadlock_timeout by setting a global flag and triggering the process latch, designed to run safely within a signal handler context.

## Definition
void CheckDeadLockAlert(void)

## Detailed Description
CheckDeadLockAlert is a signal handler function that responds to deadlock timeout expiry. It operates in two phases: first, it sets the global flag got_deadlock_timeout to true to indicate that a deadlock timeout has occurred, then it sets the process latch (MyLatch) to wake up any waiting processes. The function is designed to be signal-safe, carefully preserving errno and using only async-signal-safe operations. The latch setting is done redundantly to ensure the process wakes up even if the latch was previously set before got_deadlock_timeout was updated.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [SetLatch](../S/SetLatch.md)
- Called from (representative examples):
  - [ProcessRecoveryConflictInterrupt](../P/ProcessRecoveryConflictInterrupt.md)
  - [InitPostgres](../I/InitPostgres.md)

## Notes and Other Information
- Runs inside a signal handler, so it must be async-signal-safe
- Sets got_deadlock_timeout flag before setting the latch to ensure proper signaling
- Preserves errno value to avoid interfering with interrupted code
- The redundant latch setting is intentional to handle race conditions
- May be called within procsignal_sigusr1_handler() context where the handler also sets the latch

## Simplified Source

```c
// Simplified version of CheckDeadLockAlert
void CheckDeadLockAlert(void) {
    int save_errno = errno;

    // Set flag to indicate deadlock timeout occurred
    got_deadlock_timeout = true;

    // Wake up any waiting processes (redundant setting is intentional)
    SetLatch(MyLatch);

    // Restore errno for signal safety
    errno = save_errno;
}
```

Key simplifications made:
- This function is already very simple, so minimal simplification was needed
- Added descriptive comments explaining each operation's purpose
- Maintained the exact same logic including errno preservation for signal safety
- Kept the redundant latch setting as it's intentional for race condition handling
- The signal-safe design cannot be simplified without compromising correctness