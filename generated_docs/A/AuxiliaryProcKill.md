# AuxiliaryProcKill

## Location
[src/backend/storage/lmgr/proc.c:972-1022](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/proc.c#L972-L1022)

## Overview
Cut-down version of ProcKill specifically designed for auxiliary processes (bgwriter, checkpointer, etc.), marking the PGPROC as not-in-use without releasing it to freelists.

## Definition

```c
static void
AuxiliaryProcKill(int code, Datum arg)
```
## Detailed Description
AuxiliaryProcKill is a specialized cleanup function for auxiliary processes in PostgreSQL. Unlike regular backend processes handled by ProcKill, auxiliary processes have a simpler lifecycle and use pre-allocated PGPROC structures that are not returned to freelists upon termination.

The function performs essential cleanup operations:
1. Safety validation to ensure it's not called in child processes
2. Verification that the correct auxiliary process type is being terminated
3. Release of any held LW locks and condition variables
4. Latch ownership transfer back to local process
5. PGPROC structure marking as unused (but not deallocated)
6. Global statistics updates

Key differences from ProcKill:
- No lock group management (auxiliary processes don't participate in lock groups)
- No postmaster notification (auxiliary processes have different lifecycle management)
- PGPROC structure is marked as unused but remains allocated
- No freelist operations since auxiliary PGPROCs are statically allocated

## Parameters / Member Variables
- : Exit code (unused in this function but required by exit callback interface)
- : Datum containing the auxiliary process type identifier, extracted using DatumGetInt32

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - [LWLockReleaseAll](../L/LWLockReleaseAll.md)
  - [ConditionVariableCancelSleep](../C/ConditionVariableCancelSleep.md)
  - [SwitchBackToLocalLatch](../S/SwitchBackToLocalLatch.md)
  - [pgstat_reset_wait_event_storage](../p/pgstat_reset_wait_event_storage.md)
  - [DisownLatch](../D/DisownLatch.md)
  - [update_spins_per_delay](../u/update_spins_per_delay.md)
  - NUM_AUXILIARY_PROCS (constant)
  - INVALID_PROC_NUMBER (constant)
- Called from (representative examples):
  - [InitAuxiliaryProcess](../I/InitAuxiliaryProcess.md) (registered as exit callback)

## Notes and Other Information
- This is a static function, only accessible within proc.c
- Designed specifically for auxiliary processes like background writer, checkpointer, WAL writer, etc.
- The function uses the arg parameter to identify which auxiliary process type is terminating
- Auxiliary PGPROCs are pre-allocated in the AuxiliaryProcs array and are not dynamically managed
- Includes safety checks to prevent execution in child processes created by system() calls
- Updates global spin delay statistics as part of cleanup process
- The PGPROC structure remains allocated for potential reuse by the same auxiliary process type
- No complex lock group or postmaster interaction logic since auxiliary processes operate differently from regular backends

## Simplified Source

```c
// Simplified version of AuxiliaryProcKill
static void AuxiliaryProcKill(int exit_code, Datum process_type_arg) {
    int aux_process_type = DatumGetInt32(process_type_arg);
    PGPROC *current_proc;

    Assert(aux_process_type >= 0 && aux_process_type < NUM_AUXILIARY_PROCS);

    // Step 1: Safety check - ensure not called in child process
    if (MyProc->pid != (int) getpid()) {
        elog(PANIC, "AuxiliaryProcKill() called in child process");
    }

    Assert(MyProc == &AuxiliaryProcs[aux_process_type]);

    // Step 2: Release any held locks and cancel condition variable waits
    LWLockReleaseAll();
    ConditionVariableCancelSleep();

    // Step 3: Reset latch ownership and wait event storage
    SwitchBackToLocalLatch();
    pgstat_reset_wait_event_storage();

    // Step 4: Clear global process references
    current_proc = MyProc;
    MyProc = NULL;
    MyProcNumber = INVALID_PROC_NUMBER;
    DisownLatch(&current_proc->procLatch);

    // Step 5: Mark auxiliary process as not-in-use (but keep allocated)
    SpinLockAcquire(ProcStructLock);

    current_proc->pid = 0;  // Mark as unused
    current_proc->vxid.procNumber = INVALID_PROC_NUMBER;
    current_proc->vxid.lxid = InvalidTransactionId;

    // Update global spin delay statistics
    ProcGlobal->spins_per_delay = update_spins_per_delay(ProcGlobal->spins_per_delay);

    SpinLockRelease(ProcStructLock);
}
```

Key simplifications made:
- Renamed parameters for clarity (code -> exit_code, arg -> process_type_arg)
- Renamed variables for clarity (proctype -> aux_process_type, proc -> current_proc)
- Added step-by-step comments organizing the cleanup phases
- Removed debug-only variable (auxproc PG_USED_FOR_ASSERTS_ONLY)
- Grouped related operations together logically
- Maintained all essential safety checks and cleanup operations
- Preserved the auxiliary process-specific behavior (no deallocation)