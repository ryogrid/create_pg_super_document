# ProcKill

## Location
[src/backend/storage/lmgr/proc.c:839-971](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/proc.c#L839-L971)

## Overview
Destroys the per-process data structure for the current process, releasing held LW locks and performing comprehensive cleanup during process termination.

## Definition

```c
structure (and semaphore) to appropriate freelist */
		dlist_push_tail(procgloballist, &proc->links);
```
## Detailed Description
ProcKill is a comprehensive process cleanup function that handles the orderly termination of a PostgreSQL backend process. This function performs extensive cleanup operations to ensure that no resources are leaked and shared data structures remain consistent when a process exits.

Key cleanup operations include:
1. Safety checks to ensure it's not called in a child process created by system()
2. Cleanup from synchronous replication lists
3. Assertion checks for proper lock release in debug builds
4. Release of any remaining LW locks and condition variables
5. Complex lock group management and cleanup
6. Latch ownership transfer back to local process
7. PGPROC structure cleanup and return to freelists
8. Postmaster notification for proper process lifecycle management
9. Autovacuum launcher notification when needed

The function handles sophisticated scenarios like lock group leadership transfer when the leader exits before group members.

## Parameters / Member Variables
- : Exit code (unused in this function but required by exit callback interface)
- : Datum argument (unused in this function but required by exit callback interface)

## Dependencies
- Functions called/Symbols referenced:
  - [SyncRepCleanupAtProcExit](../S/SyncRepCleanupAtProcExit.md)
  - [dlist_is_empty](../d/dlist_is_empty.md)
  - [LWLockReleaseAll](../L/LWLockReleaseAll.md)
  - [ConditionVariableCancelSleep](../C/ConditionVariableCancelSleep.md)
  - LockHashPartitionLockByProc
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [dlist_delete](../d/dlist_delete.md)
  - [dlist_push_head](../d/dlist_push_head.md)
  - [SwitchBackToLocalLatch](../S/SwitchBackToLocalLatch.md)
  - [pgstat_reset_wait_event_storage](../p/pgstat_reset_wait_event_storage.md)
  - [DisownLatch](../D/DisownLatch.md)
  - [dlist_push_tail](../d/dlist_push_tail.md)
  - [update_spins_per_delay](../u/update_spins_per_delay.md)
  - [MarkPostmasterChildInactive](../M/MarkPostmasterChildInactive.md)
  - AmAutoVacuumLauncherProcess
  - AmLogicalSlotSyncWorkerProcess
  - kill
- Called from (representative examples):
  - [InitProcess](../I/InitProcess.md) (registered as exit callback)

## Notes and Other Information
- This is a static function, only accessible within proc.c
- Function parameters follow the standard exit callback signature but are not used
- Includes safety check to prevent execution in child processes created by system() calls
- Handles complex lock group scenarios where leaders may exit before members
- Performs different cleanup based on whether the process is under postmaster control
- Updates global spin delay statistics as part of cleanup
- Sends SIGUSR2 to autovacuum launcher if needed to wake it up after worker termination
- The function ensures proper transfer of latch ownership to prevent resource leaks
- Registered as an exit callback during process initialization for automatic cleanup

## Simplified Source

```c
// Simplified version of ProcKill
static void ProcKill(int code, Datum arg) {
    PGPROC *proc;
    dlist_head *procgloballist;

    // Safety check: ensure not called in child process
    if (MyProc->pid != (int) getpid()) {
        elog(PANIC, "ProcKill() called in child process");
    }

    // Step 1: Clean up synchronous replication
    SyncRepCleanupAtProcExit();

    // Step 2: Release all lightweight locks and condition variables
    LWLockReleaseAll();
    ConditionVariableCancelSleep();

    // Step 3: Handle lock group cleanup
    if (MyProc->lockGroupLeader != NULL) {
        PGPROC *leader = MyProc->lockGroupLeader;
        LWLock *leader_lwlock = LockHashPartitionLockByProc(leader);

        LWLockAcquire(leader_lwlock, LW_EXCLUSIVE);
        dlist_delete(&MyProc->lockGroupLink);

        // If no more group members, clean up leader
        if (dlist_is_empty(&leader->lockGroupMembers)) {
            leader->lockGroupLeader = NULL;
            if (leader != MyProc) {
                // Return leader's PGPROC to freelist
                procgloballist = leader->procgloballist;
                SpinLockAcquire(ProcStructLock);
                dlist_push_head(procgloballist, &leader->links);
                SpinLockRelease(ProcStructLock);
            }
        }
        LWLockRelease(leader_lwlock);
    }

    // Step 4: Reset latch and clear process information
    SwitchBackToLocalLatch();
    pgstat_reset_wait_event_storage();

    proc = MyProc;
    MyProc = NULL;
    MyProcNumber = INVALID_PROC_NUMBER;
    DisownLatch(&proc->procLatch);

    // Step 5: Mark process as no longer in use
    proc->pid = 0;
    proc->vxid.procNumber = INVALID_PROC_NUMBER;
    proc->vxid.lxid = InvalidTransactionId;

    // Step 6: Return PGPROC to freelist if not a group leader
    procgloballist = proc->procgloballist;
    SpinLockAcquire(ProcStructLock);

    if (proc->lockGroupLeader == NULL) {
        dlist_push_tail(procgloballist, &proc->links);
    }

    // Update global statistics
    ProcGlobal->spins_per_delay = update_spins_per_delay(ProcGlobal->spins_per_delay);
    SpinLockRelease(ProcStructLock);

    // Step 7: Notify postmaster and autovacuum launcher
    if (IsUnderPostmaster && !AmAutoVacuumLauncherProcess() &&
        !AmLogicalSlotSyncWorkerProcess()) {
        MarkPostmasterChildInactive();
    }

    if (AutovacuumLauncherPid != 0) {
        kill(AutovacuumLauncherPid, SIGUSR2);
    }
}
```

Key simplifications made:
- Removed debug assertion code for clarity
- Consolidated complex lock group logic into essential steps
- Abstracted detailed error handling paths
- Focused on the main execution flow
- Added step-by-step comments for major phases
- Simplified conditional logic while preserving functionality