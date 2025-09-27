# CleanupProcSignalState

## Location
[src/backend/storage/ipc/procsignal.c:211-256](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procsignal.c#L211-L256)

## Overview
Removes the current process from the ProcSignal mechanism during backend shutdown, cleaning up the process signal slot and ensuring proper synchronization state for barrier operations.

## Definition

```c
static void
CleanupProcSignalState(int status, Datum arg)
```
## Detailed Description
CleanupProcSignalState is a cleanup function registered via on_shmem_exit() that is automatically called during backend process termination. It safely removes the current process from the process signaling infrastructure by first clearing the global MyProcSignalSlot pointer to prevent race conditions with signal handlers, then performing sanity checks on the slot contents. The function sets the slot's barrier generation to the maximum value to ensure no barrier operations will block waiting for this slot, broadcasts to any condition variable waiters, and finally clears the process ID to mark the slot as available for reuse.

## Parameters / Member Variables
- : Exit status code (not used in function logic)
- : Datum argument (not used in function logic)

## Dependencies
- Functions called/Symbols referenced:
  - Assert
  - elog
  - [pg_atomic_write_u64](../p/pg_atomic_write_u64.md)
  - [ConditionVariableBroadcast](ConditionVariableBroadcast.md)
  - [ProcSignalSlot](../P/ProcSignalSlot.md) (type)
  - PG_UINT64_MAX (constant)
- Called from (representative examples):
  - [ProcSignalInit](../P/ProcSignalInit.md) (registered via on_shmem_exit)
  - Automatic exit handling

## Notes and Other Information
- Declared as static function, only used internally within procsignal.c
- Uses LOG level instead of ERROR for sanity check failures to avoid infinite exit loops
- Critical for preventing barrier operations from blocking on dead processes
- Sets barrier generation to PG_UINT64_MAX to indicate the slot has absorbed all barriers
- Clears MyProcSignalSlot early to prevent SIGUSR1 handlers from accessing invalid memory
- Part of PostgreSQL's graceful shutdown and cleanup mechanism
- Located in src/backend/storage/ipc/procsignal.c:211-256

## Simplified Source

```c
// Simplified version of CleanupProcSignalState
static void CleanupProcSignalState(int status, Datum arg) {
    ProcSignalSlot *slot = MyProcSignalSlot;

    // Clear global slot pointer to prevent race conditions
    Assert(MyProcSignalSlot != NULL);
    MyProcSignalSlot = NULL;

    // Verify we own this slot before cleanup
    if (slot->pss_pid != MyProcPid) {
        elog(LOG, "process %d releasing ProcSignal slot %d, but it contains %d",
             MyProcPid, (int) (slot - ProcSignal->psh_slot), (int) slot->pss_pid);
        return;
    }

    // Signal all barriers as complete to unblock waiters
    pg_atomic_write_u64(&slot->pss_barrierGeneration, PG_UINT64_MAX);
    ConditionVariableBroadcast(&slot->pss_barrierCV);

    // Mark slot as available
    slot->pss_pid = 0;
}
```

Key simplifications made:
- Added descriptive comments for each major operation
- Clarified the barrier handling logic
- Core logic: Clear global pointer, verify ownership, signal barrier completion, free slot