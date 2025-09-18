# AbortSubTransaction

## Location
src/backend/access/transam/xact.c: 5162 - 5320

## Overview
AbortSubTransaction aborts a subtransaction by performing comprehensive cleanup operations, releasing resources, and restoring the transaction state to ensure system consistency after a subtransaction failure.

## Definition


## Detailed Description
AbortSubTransaction is a static function responsible for aborting a subtransaction in PostgreSQL's transaction management system. The function performs extensive cleanup and recovery operations:

1. **Interrupt Protection**: Prevents cancel/die interrupts during cleanup using HOLD_INTERRUPTS()
2. **Memory and Resource Setup**: Ensures valid memory context and resource owner via AtSubAbort_Memory() and AtSubAbort_ResourceOwner()
3. **Lock and Buffer Cleanup**: Releases lightweight locks, unlocks buffers, and cleans up lock wait states
4. **System State Reset**: Resets WAL insertion state, cancels condition variable sleeps, and reschedules timeouts
5. **Signal Handling**: Re-enables signals that may have been blocked due to longjmp from signal handlers
6. **State Validation**: Checks transaction state and transitions to TRANS_ABORT
7. **User Context Reset**: Restores previous user ID and security context
8. **Subsystem Cleanup**: Resets REINDEX state and logical streaming state
9. **Parallel Operations**: Cleans up any unfinished parallel operations without warnings
10. **Resource Owner Processing**: If a resource owner exists, performs comprehensive cleanup including:
    - Triggers, portals, large objects cleanup
    - Transaction abort recording in pg_xact
    - Child XIDs and callback processing
    - Resource release in multiple phases
    - Subsystem-specific abort handlers
11. **State Restoration**: Restores read-only state and resumes interrupts

The function is designed to handle partial failures gracefully, especially when a subtransaction fails before creating a ResourceOwner.

## Parameters / Member Variables
This function takes no parameters and operates on the global CurrentTransactionState.

## Dependencies
- Functions called/Symbols referenced:
  - HOLD_INTERRUPTS, RESUME_INTERRUPTS
  - AtSubAbort_Memory, AtSubAbort_ResourceOwner
  - LWLockReleaseAll, UnlockBuffers
  - XLogResetInsertion
  - ConditionVariableCancelSleep
  - LockErrorCleanup, reschedule_timeouts
  - sigprocmask
  - ShowTransactionState, TransStateAsString
  - SetUserIdAndSecContext
  - ResetReindexState, ResetLogicalStreamingState
  - AtEOSubXact_Parallel
  - AfterTriggerEndSubXact
  - AtSubAbort_Portals
  - AtEOSubXact_LargeObject
  - AtSubAbort_Notify
  - RecordTransactionAbort
  - AtSubAbort_childXids
  - CallSubXactCallbacks
  - ResourceOwnerRelease
  - AtEOSubXact_RelationCache, AtEOSubXact_Inval
  - AtSubAbort_smgr
  - AtEOXact_GUC, AtEOSubXact_SPI
  - AtEOSubXact_on_commit_actions, AtEOSubXact_Namespace
  - AtEOSubXact_Files, AtEOSubXact_HashTables
  - AtEOSubXact_PgStat
  - AtSubAbort_Snapshot
- Called from (representative examples):
  - CommitTransactionCommandInternal
  - AbortCurrentTransactionInternal
  - RollbackAndReleaseCurrentSubTransaction
  - AbortOutOfAnyTransaction

## Notes and Other Information
- The function includes a FIXME comment questioning whether some locks (like buffer locks) should be kept during abort
- Unlike CommitSubTransaction, this function does not require snapshot export handling since exports are not supported in subtransactions
- The function handles the case where a subtransaction fails before creating a ResourceOwner by skipping resource-dependent cleanup
- Signal handling is carefully managed to ensure timeout infrastructure remains functional during abort processing
- The function records the transaction abort in pg_xact to advertise the abort to other processes
- Parallel operation cleanup is performed without warnings, unlike in the commit case
- Located in src/backend/access/transam/xact.c:5162-5320