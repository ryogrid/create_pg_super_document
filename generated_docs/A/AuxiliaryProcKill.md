# AuxiliaryProcKill

## Location
src/backend/storage/lmgr/proc.c: 972 - 1022

## Overview
Cut-down version of ProcKill specifically designed for auxiliary processes (bgwriter, checkpointer, etc.), marking the PGPROC as not-in-use without releasing it to freelists.

## Definition


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
  - DatumGetInt32
  - LWLockReleaseAll
  - ConditionVariableCancelSleep
  - SwitchBackToLocalLatch
  - pgstat_reset_wait_event_storage
  - DisownLatch
  - update_spins_per_delay
  - NUM_AUXILIARY_PROCS (constant)
  - INVALID_PROC_NUMBER (constant)
- Called from (representative examples):
  - InitAuxiliaryProcess (registered as exit callback)

## Notes and Other Information
- This is a static function, only accessible within proc.c
- Designed specifically for auxiliary processes like background writer, checkpointer, WAL writer, etc.
- The function uses the arg parameter to identify which auxiliary process type is terminating
- Auxiliary PGPROCs are pre-allocated in the AuxiliaryProcs array and are not dynamically managed
- Includes safety checks to prevent execution in child processes created by system() calls
- Updates global spin delay statistics as part of cleanup process
- The PGPROC structure remains allocated for potential reuse by the same auxiliary process type
- No complex lock group or postmaster interaction logic since auxiliary processes operate differently from regular backends