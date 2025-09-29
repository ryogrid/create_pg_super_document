# CommitTransaction

## Location
[src/bin/pg_dump/pg_backup_db.c:537-551](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_db.c#L537-L551)

## Overview
CommitTransaction is the core function responsible for committing a PostgreSQL transaction, handling all necessary cleanup and finalization steps to ensure data consistency and proper resource management.

## Definition

```c
void
CommitTransaction(Archive *AHX)
```
## Detailed Description
CommitTransaction performs the complete commit sequence for a PostgreSQL transaction. It orchestrates a complex series of operations in a carefully ordered sequence to ensure ACID properties are maintained. The function handles both regular transactions and parallel worker transactions, with special logic for each case.

The commit process is divided into several phases:
1. Pre-commit processing that may involve user-defined code (triggers, portals)
2. Resource cleanup and synchronization 
3. Durability operations (WAL logging, relation map updates)
4. Post-commit cleanup and resource release
5. Transaction state reset

The function includes special handling for parallel workers, where the parallel leader is responsible for certain operations like marking XIDs as committed.

## Parameters / Member Variables
This function takes no parameters but operates on global transaction state, particularly:
- : The current transaction's state structure
- : Boolean indicating if this is a parallel worker transaction

## Dependencies
- Functions called/Symbols referenced:
  - [ShowTransactionState](../S/ShowTransactionState.md)
  - [AfterTriggerFireDeferred](../A/AfterTriggerFireDeferred.md)
  - [PreCommit_Portals](../P/PreCommit_Portals.md)
  - [CallXactCallbacks](CallXactCallbacks.md)
  - [AtEOXact_Parallel](../A/AtEOXact_Parallel.md)
  - [AfterTriggerEndXact](../A/AfterTriggerEndXact.md)
  - [PreCommit_on_commit_actions](../P/PreCommit_on_commit_actions.md)
  - [smgrDoPendingSyncs](../s/smgrDoPendingSyncs.md)
  - [AtEOXact_LargeObject](../A/AtEOXact_LargeObject.md)
  - [PreCommit_Notify](../P/PreCommit_Notify.md)
  - [PreCommit_CheckForSerializationFailure](../P/PreCommit_CheckForSerializationFailure.md)
  - [AtEOXact_RelationMap](../A/AtEOXact_RelationMap.md)
  - [RecordTransactionCommit](../R/RecordTransactionCommit.md)
  - [ProcArrayEndTransaction](../P/ProcArrayEndTransaction.md)
  - [ResourceOwnerRelease](../R/ResourceOwnerRelease.md)
  - [AtEOXact_Buffers](../A/AtEOXact_Buffers.md)
  - [AtEOXact_RelationCache](../A/AtEOXact_RelationCache.md)
  - [AtEOXact_Inval](../A/AtEOXact_Inval.md)
  - [AtEOXact_MultiXact](../A/AtEOXact_MultiXact.md)
  - [smgrDoPendingDeletes](../s/smgrDoPendingDeletes.md)
  - [AtCommit_Notify](../A/AtCommit_Notify.md)
  - [AtEOXact_GUC](../A/AtEOXact_GUC.md)
  - [AtEOXact_SPI](../A/AtEOXact_SPI.md)
  - [AtEOXact_Enum](../A/AtEOXact_Enum.md)
  - [AtEOXact_on_commit_actions](../A/AtEOXact_on_commit_actions.md)
  - [AtEOXact_Namespace](../A/AtEOXact_Namespace.md)
  - [AtEOXact_SMgr](../A/AtEOXact_SMgr.md)
  - [AtEOXact_Files](../A/AtEOXact_Files.md)
  - [AtEOXact_ComboCid](../A/AtEOXact_ComboCid.md)
  - [AtEOXact_HashTables](../A/AtEOXact_HashTables.md)
  - [AtEOXact_PgStat](../A/AtEOXact_PgStat.md)
  - [AtEOXact_Snapshot](../A/AtEOXact_Snapshot.md)
  - [AtEOXact_ApplyLauncher](../A/AtEOXact_ApplyLauncher.md)
  - [AtEOXact_LogicalRepWorkers](../A/AtEOXact_LogicalRepWorkers.md)
  - [AtCommit_Memory](../A/AtCommit_Memory.md)

- Called from (representative examples):
  - [CommitTransactionCommandInternal](CommitTransactionCommandInternal.md)
  - [EndParallelWorkerTransaction](../E/EndParallelWorkerTransaction.md)
  - [RestoreArchive](../R/RestoreArchive.md) (pg_dump)
  - [restore_toc_entry](../r/restore_toc_entry.md) (pg_dump)
  - [IssueCommandPerBlob](../I/IssueCommandPerBlob.md) (pg_dump)

## Notes and Other Information
- This is a static function within xact.c, meaning it's only called from within the transaction management module
- The function includes extensive comments noting that changes here should also be considered for PrepareTransaction
- The ordering of cleanup operations is critical - resources visible to other backends are released first, then locks, then backend-local resources
- Special handling for parallel workers ensures that only the parallel leader performs certain operations like marking XIDs as committed
- The function uses HOLD_INTERRUPTS/RESUME_INTERRUPTS to prevent cancellation during critical cleanup phases
- Error handling switches to transaction abort path if errors occur during most of the commit process
- File location: src/backend/access/transam/xact.c:2178-2459

## Simplified Source

```c
// Simplified version of CommitTransaction
static void
CommitTransaction(void)
{
    TransactionState s = CurrentTransactionState;
    TransactionId latestXid;
    bool is_parallel_worker;

    is_parallel_worker = (s->blockState == TBLOCK_PARALLEL_INPROGRESS);

    // Enforce parallel mode restrictions for parallel workers
    if (is_parallel_worker)
        EnterParallelMode();

    // Validate transaction state
    if (s->state != TRANS_INPROGRESS)
        elog(WARNING, "CommitTransaction while in %s state",
             TransStateAsString(s->state));

    // Phase 1: Pre-commit processing with user-defined code
    // Loop until all deferred triggers and portals are processed
    for (;;) {
        AfterTriggerFireDeferred();
        if (!PreCommit_Portals(false))
            break;
    }

    // Phase 2: Pre-commit callbacks and cleanup
    CallXactCallbacks(is_parallel_worker ? XACT_EVENT_PARALLEL_PRE_COMMIT
                                         : XACT_EVENT_PRE_COMMIT);

    // Clean up parallel operations and validate parallel mode level
    AtEOXact_Parallel(true);

    // Shut down trigger manager and handle ON COMMIT actions
    AfterTriggerEndXact(true);
    PreCommit_on_commit_actions();

    // Synchronize files and handle large objects
    smgrDoPendingSyncs(true, is_parallel_worker);
    AtEOXact_LargeObject(true);

    // Handle notifications and serialization
    PreCommit_Notify();
    if (!is_parallel_worker)
        PreCommit_CheckForSerializationFailure();

    // Phase 3: Critical commit section (no interrupts)
    HOLD_INTERRUPTS();

    // Update relation map and set transaction state to committing
    AtEOXact_RelationMap(true, is_parallel_worker);
    s->state = TRANS_COMMIT;
    s->parallelModeLevel = 0;

    // Disable transaction timeout
    if (TransactionTimeout > 0)
        disable_timeout(TRANSACTION_TIMEOUT, false);

    // Phase 4: Durability - record transaction commit
    if (!is_parallel_worker) {
        // Mark XIDs as committed in pg_xact (durability point)
        latestXid = RecordTransactionCommit();
    } else {
        // Parallel workers don't mark XID - leader handles this
        latestXid = InvalidTransactionId;
        ParallelWorkerReportLastRecEnd(XactLastRecEnd);
    }

    // Signal end of transaction to other processes
    ProcArrayEndTransaction(MyProc, latestXid);

    // Phase 5: Post-commit cleanup (ordered resource release)
    CallXactCallbacks(is_parallel_worker ? XACT_EVENT_PARALLEL_COMMIT
                                         : XACT_EVENT_COMMIT);

    // Release resources in order: visible resources, locks, local resources
    CurrentResourceOwner = NULL;
    ResourceOwnerRelease(TopTransactionResourceOwner,
                        RESOURCE_RELEASE_BEFORE_LOCKS, true, true);

    // Clean up buffers, relation cache, and invalidation
    AtEOXact_Buffers(true);
    AtEOXact_RelationCache(true);
    AtEOXact_Inval(true);
    AtEOXact_MultiXact();

    // Release locks and remaining resources
    ResourceOwnerRelease(TopTransactionResourceOwner,
                        RESOURCE_RELEASE_LOCKS, true, true);
    ResourceOwnerRelease(TopTransactionResourceOwner,
                        RESOURCE_RELEASE_AFTER_LOCKS, true, true);

    // Delete files and send notifications
    smgrDoPendingDeletes(true);
    AtCommit_Notify();

    // Phase 6: Backend-internal cleanup
    AtEOXact_GUC(true, 1);
    AtEOXact_SPI(true);
    AtEOXact_Enum();
    AtEOXact_on_commit_actions(true);
    AtEOXact_Namespace(true, is_parallel_worker);
    AtEOXact_SMgr();
    AtEOXact_Files(true);
    AtEOXact_ComboCid();
    AtEOXact_HashTables(true);
    AtEOXact_PgStat(true, is_parallel_worker);
    AtEOXact_Snapshot(true, false);
    AtEOXact_ApplyLauncher(true);
    AtEOXact_LogicalRepWorkers(true);

    // Final cleanup and state reset
    ResourceOwnerDelete(TopTransactionResourceOwner);
    s->curTransactionOwner = NULL;
    CurTransactionResourceOwner = NULL;
    TopTransactionResourceOwner = NULL;

    AtCommit_Memory();

    // Reset transaction identifiers and state
    s->fullTransactionId = InvalidFullTransactionId;
    s->subTransactionId = InvalidSubTransactionId;
    s->nestingLevel = 0;
    s->gucNestLevel = 0;
    s->childXids = NULL;
    s->nChildXids = 0;
    s->maxChildXids = 0;

    XactTopFullTransactionId = InvalidFullTransactionId;
    nParallelCurrentXids = 0;

    // Transaction complete - return to default state
    s->state = TRANS_DEFAULT;

    RESUME_INTERRUPTS();
}
```

Key simplifications made:
- Organized code into clear phases with descriptive comments
- Removed detailed error handling warnings for parallel mode level validation
- Consolidated similar AtEOXact_* cleanup calls with brief explanatory comments
- Abstracted complex conditional logic into simpler flow descriptions
- Simplified variable initialization and state management
- Removed tracing calls and detailed debugging output
- Grouped related operations together for better understanding of the commit sequence
- Emphasized the critical ordering of resource release operations