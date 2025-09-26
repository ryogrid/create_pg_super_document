# AbortSubTransaction

## Location
[src/backend/access/transam/xact.c:5162-5320](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L5162-L5320)

## Overview
AbortSubTransaction aborts a subtransaction by performing comprehensive cleanup operations, releasing resources, and restoring the transaction state to ensure system consistency after a subtransaction failure.

## Definition

```c
struction state */
	XLogResetInsertion();
```
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
  - [AtSubAbort_Memory](AtSubAbort_Memory.md), AtSubAbort_ResourceOwner
  - [LWLockReleaseAll](../L/LWLockReleaseAll.md), UnlockBuffers
  - [XLogResetInsertion](../X/XLogResetInsertion.md)
  - [ConditionVariableCancelSleep](../C/ConditionVariableCancelSleep.md)
  - [LockErrorCleanup](../L/LockErrorCleanup.md), reschedule_timeouts
  - sigprocmask
  - [ShowTransactionState](../S/ShowTransactionState.md), TransStateAsString
  - [SetUserIdAndSecContext](../S/SetUserIdAndSecContext.md)
  - [ResetReindexState](../R/ResetReindexState.md), ResetLogicalStreamingState
  - [AtEOSubXact_Parallel](AtEOSubXact_Parallel.md)
  - [AfterTriggerEndSubXact](AfterTriggerEndSubXact.md)
  - [AtSubAbort_Portals](AtSubAbort_Portals.md)
  - [AtEOSubXact_LargeObject](AtEOSubXact_LargeObject.md)
  - [AtSubAbort_Notify](AtSubAbort_Notify.md)
  - [RecordTransactionAbort](../R/RecordTransactionAbort.md)
  - [AtSubAbort_childXids](AtSubAbort_childXids.md)
  - [CallSubXactCallbacks](../C/CallSubXactCallbacks.md)
  - [ResourceOwnerRelease](../R/ResourceOwnerRelease.md)
  - [AtEOSubXact_RelationCache](AtEOSubXact_RelationCache.md), AtEOSubXact_Inval
  - [AtSubAbort_smgr](AtSubAbort_smgr.md)
  - [AtEOXact_GUC](AtEOXact_GUC.md), AtEOSubXact_SPI
  - [AtEOSubXact_on_commit_actions](AtEOSubXact_on_commit_actions.md), AtEOSubXact_Namespace
  - [AtEOSubXact_Files](AtEOSubXact_Files.md), AtEOSubXact_HashTables
  - [AtEOSubXact_PgStat](AtEOSubXact_PgStat.md)
  - [AtSubAbort_Snapshot](AtSubAbort_Snapshot.md)
- Called from (representative examples):
  - [CommitTransactionCommandInternal](../C/CommitTransactionCommandInternal.md)
  - [AbortCurrentTransactionInternal](AbortCurrentTransactionInternal.md)
  - [RollbackAndReleaseCurrentSubTransaction](../R/RollbackAndReleaseCurrentSubTransaction.md)
  - [AbortOutOfAnyTransaction](AbortOutOfAnyTransaction.md)

## Notes and Other Information
- The function includes a FIXME comment questioning whether some locks (like buffer locks) should be kept during abort
- Unlike CommitSubTransaction, this function does not require snapshot export handling since exports are not supported in subtransactions
- The function handles the case where a subtransaction fails before creating a ResourceOwner by skipping resource-dependent cleanup
- Signal handling is carefully managed to ensure timeout infrastructure remains functional during abort processing
- The function records the transaction abort in pg_xact to advertise the abort to other processes
- Parallel operation cleanup is performed without warnings, unlike in the commit case
- Located in src/backend/access/transam/xact.c:5162-5320