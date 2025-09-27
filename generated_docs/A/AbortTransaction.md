# AbortTransaction

## Location
[src/backend/access/transam/xact.c:2749-2944](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L2749-L2944)

## Overview
AbortTransaction handles the complete rollback process for a PostgreSQL transaction, performing comprehensive cleanup of all transaction-related resources and state.

## Definition

```c
struction state */
	XLogResetInsertion();
```
## Detailed Description
AbortTransaction is responsible for safely aborting a transaction and cleaning up all associated resources. The function handles both regular transactions and parallel worker transactions, performing critical emergency cleanup before systematic resource deallocation.

The abort process follows a carefully designed sequence:
1. **Emergency cleanup**: Immediately releases lightweight locks, clears wait states, resets WAL state, and re-enables signals
2. **State validation**: Verifies the transaction is in an abortable state (TRANS_INPROGRESS or TRANS_PREPARE)  
3. **Security context reset**: Restores user ID and security context to handle cases where abort occurs during SECURITY DEFINER functions
4. **Subsystem cleanup**: Resets various subsystem states (REINDEX, logical streaming, snapshot export)
5. **Abort processing**: Calls abort-specific cleanup routines for triggers, portals, storage, etc.
6. **WAL recording**: Records the transaction abort (except for parallel workers)
7. **Resource release**: Systematically releases resources in the proper order
8. **Final cleanup**: Performs end-of-transaction cleanup for all subsystems

The transaction state transitions to TRANS_ABORT and remains there until CleanupTransaction() is called.

## Parameters / Member Variables
This function takes no parameters but operates on:
- : The current transaction's state structure
- : Used to detect parallel worker mode (TBLOCK_PARALLEL_INPROGRESS)
- : Transaction state that must be TRANS_INPROGRESS or TRANS_PREPARE
- : Transaction ID returned by RecordTransactionAbort()
- : Flag indicating if this is a parallel worker transaction

## Dependencies
- Functions called/Symbols referenced:
  - [AtAbort_Memory](AtAbort_Memory.md)/AtAbort_ResourceOwner (emergency memory/resource cleanup)
  - [LWLockReleaseAll](../L/LWLockReleaseAll.md) (release all lightweight locks immediately)
  - [RecordTransactionAbort](../R/RecordTransactionAbort.md) (record abort in WAL, except for parallel workers)
  - [SetUserIdAndSecContext](../S/SetUserIdAndSecContext.md) (restore user ID and security context)
  - AtEOXact_* functions (end-of-transaction cleanup for various subsystems)
  - AtAbort_* functions (abort-specific cleanup routines)
  - [ResourceOwnerRelease](../R/ResourceOwnerRelease.md) (systematic resource cleanup)
  - [ProcArrayEndTransaction](../P/ProcArrayEndTransaction.md) (remove from process array)

- Called from (representative examples):
  - [AbortCurrentTransactionInternal](AbortCurrentTransactionInternal.md) (various error recovery scenarios)
  - [AbortOutOfAnyTransaction](AbortOutOfAnyTransaction.md) (emergency abort from any transaction state)
  - [CommitTransactionCommandInternal](../C/CommitTransactionCommandInternal.md) (when commit preparation fails)

## Notes and Other Information
- Uses HOLD_INTERRUPTS/RESUME_INTERRUPTS to prevent interruption during critical cleanup
- Immediately releases lightweight locks since they might be needed during cleanup
- Restores signal mask early to ensure timeout infrastructure works during abort
- Parallel workers don't record their own abort but nudge WAL-writer for LSN reporting
- Transaction state remains TRANS_ABORT until CleanupTransaction() resets it to TRANS_DEFAULT
- Resource cleanup follows the same ordering principles as CommitTransaction for consistency
- Can skip resource cleanup if the transaction failed before creating a resource owner
- Handles both normal transaction aborts and aborts during two-phase commit preparation

## Simplified Source

```c
// Simplified version of AbortTransaction
static void AbortTransaction(void) {
    TransactionState s = CurrentTransactionState;
    TransactionId latestXid;
    bool is_parallel_worker;

    // Prevent interrupts during cleanup
    HOLD_INTERRUPTS();

    // Disable transaction timeout
    if (TransactionTimeout > 0) {
        disable_timeout(TRANSACTION_TIMEOUT, false);
    }

    // Setup memory context and resource owner
    AtAbort_Memory();
    AtAbort_ResourceOwner();

    // Emergency cleanup - release locks and reset state
    LWLockReleaseAll();
    pgstat_report_wait_end();
    pgstat_progress_end_command();
    UnlockBuffers();
    XLogResetInsertion();
    ConditionVariableCancelSleep();
    LockErrorCleanup();
    reschedule_timeouts();

    // Re-enable signals for timeout infrastructure
    sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

    // Validate transaction state
    is_parallel_worker = (s->blockState == TBLOCK_PARALLEL_INPROGRESS);
    if (s->state != TRANS_INPROGRESS && s->state != TRANS_PREPARE) {
        elog(WARNING, "AbortTransaction while in %s state",
             TransStateAsString(s->state));
    }
    s->state = TRANS_ABORT;

    // Restore user context and reset subsystem state
    SetUserIdAndSecContext(s->prevUser, s->prevSecContext);
    ResetReindexState(s->nestingLevel);
    ResetLogicalStreamingState();
    SnapBuildResetExportedSnapshotState();

    // Clean up parallel operations
    AtEOXact_Parallel(false);
    s->parallelModeLevel = 0;
    s->parallelChildXact = false;

    // Core abort processing
    AfterTriggerEndXact(false);
    AtAbort_Portals();
    smgrDoPendingSyncs(false, is_parallel_worker);
    AtEOXact_LargeObject(false);
    AtAbort_Notify();
    AtEOXact_RelationMap(false, is_parallel_worker);
    AtAbort_Twophase();

    // Record transaction abort (except for parallel workers)
    if (!is_parallel_worker) {
        latestXid = RecordTransactionAbort(false);
    } else {
        latestXid = InvalidTransactionId;
        XLogSetAsyncXactLSN(XactLastRecEnd);
    }

    // Update process array
    ProcArrayEndTransaction(MyProc, latestXid);

    // Post-abort cleanup - only if resource owner exists
    if (TopTransactionResourceOwner != NULL) {
        // Call appropriate callbacks
        if (is_parallel_worker) {
            CallXactCallbacks(XACT_EVENT_PARALLEL_ABORT);
        } else {
            CallXactCallbacks(XACT_EVENT_ABORT);
        }

        // Release resources in phases
        ResourceOwnerRelease(TopTransactionResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
        AtEOXact_Buffers(false);
        AtEOXact_RelationCache(false);
        AtEOXact_Inval(false);
        AtEOXact_MultiXact();
        ResourceOwnerRelease(TopTransactionResourceOwner, RESOURCE_RELEASE_LOCKS, false, true);
        ResourceOwnerRelease(TopTransactionResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, false, true);
        smgrDoPendingDeletes(false);

        // Clean up various subsystems
        AtEOXact_GUC(false, 1);
        AtEOXact_SPI(false);
        AtEOXact_Enum();
        AtEOXact_on_commit_actions(false);
        AtEOXact_Namespace(false, is_parallel_worker);
        AtEOXact_SMgr();
        AtEOXact_Files(false);
        AtEOXact_ComboCid();
        AtEOXact_HashTables(false);
        AtEOXact_PgStat(false, is_parallel_worker);
        AtEOXact_ApplyLauncher(false);
        AtEOXact_LogicalRepWorkers(false);
        pgstat_report_xact_timestamp(0);
    }

    // State remains TRANS_ABORT until CleanupTransaction()
    RESUME_INTERRUPTS();
}
```

Key simplifications made:
- Grouped emergency cleanup operations together
- Simplified parallel worker detection and handling
- Consolidated resource release phases
- Removed detailed comments while preserving logical flow
- Organized subsystem cleanup calls by functional groups