# PrepareTransaction

## Location
[src/backend/access/transam/xact.c:2460-2748](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L2460-L2748)

## Overview
PrepareTransaction implements the first phase of PostgreSQL's two-phase commit protocol, preparing a transaction for later commit or rollback while maintaining its state in persistent storage.

## Definition

```c
static void
PrepareTransaction(void)
```
## Detailed Description
PrepareTransaction executes the "prepare" phase of a two-phase commit, which involves saving the transaction's state to persistent storage while keeping it uncommitted. This allows the transaction to survive system crashes and be later committed or rolled back by another process.

The function performs comprehensive validation and preparation:
- Executes the same pre-commit processing as CommitTransaction (triggers, portal cleanup)
- Validates that the transaction doesn't use temporary objects or exported snapshots (both are incompatible with two-phase commit)
- Records the transaction state using the two-phase commit infrastructure
- Transfers locks and resources from the current backend to a dummy PGPROC entry
- Detaches the transaction from the current backend while keeping it alive globally

The transaction transitions through states: TRANS_INPROGRESS → TRANS_PREPARE → TRANS_DEFAULT, but remains globally active in the prepared state.

## Parameters / Member Variables
This function takes no parameters but operates on:
- : The current transaction's state structure  
- : Transaction ID obtained via GetCurrentTransactionId()
- : Global transaction entry created by MarkAsPreparing()
- : Global identifier for the prepared transaction (from external context)
- : Timestamp when preparation occurred

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTransactionId](../G/GetCurrentTransactionId.md) (obtains the current XID)
  - [MarkAsPreparing](../M/MarkAsPreparing.md) (reserves GID and creates global transaction entry)
  - [StartPrepare](../S/StartPrepare.md)/EndPrepare (manages two-phase state file creation)
  - AtPrepare_* functions (collect data for two-phase state file)
  - PostPrepare_* functions (clean up after preparation)
  - [PostPrepare_Twophase](PostPrepare_Twophase.md) (completes transaction detachment)
  - [ProcArrayClearTransaction](ProcArrayClearTransaction.md) (removes from process array)
  
- Called from (representative examples):
  - [CommitTransactionCommandInternal](../C/CommitTransactionCommandInternal.md) (when processing PREPARE TRANSACTION command)

## Notes and Other Information
- Contains the same warning as CommitTransaction about coordinating changes between the two functions
- Explicitly prohibits preparing transactions that accessed temporary objects or exported snapshots
- Uses the same pre-commit loop as CommitTransaction to handle triggers and portals
- Resource cleanup follows a specific order to ensure proper two-phase commit semantics
- The prepared transaction can survive backend termination and system crashes
- After PostPrepare_Twophase(), the transaction is completely detached from the current backend
- Treats PREPARE like ROLLBACK for some subsystems (apply launcher, logical replication workers) since the transaction isn't yet committed

## Simplified Source

```c
// Simplified version of PrepareTransaction
static void
PrepareTransaction(void)
{
    TransactionState s = CurrentTransactionState;
    TransactionId xid = GetCurrentTransactionId();
    GlobalTransaction gxact;
    TimestampTz prepared_at;

    // Validate current transaction state
    if (s->state != TRANS_INPROGRESS)
        elog(WARNING, "PrepareTransaction while in %s state", TransStateAsString(s->state));

    // Phase 1: Pre-commit processing loop (triggers, portals)
    for (;;) {
        AfterTriggerFireDeferred();
        if (!PreCommit_Portals(true))
            break;  // No more portals to process
    }

    CallXactCallbacks(XACT_EVENT_PRE_PREPARE);

    // Phase 2: Cleanup that can't call user code
    AfterTriggerEndXact(true);
    PreCommit_on_commit_actions();
    smgrDoPendingSyncs(true, false);  // Sync non-WAL-logged files
    AtEOXact_LargeObject(true);
    PreCommit_CheckForSerializationFailure();

    // Phase 3: Validation checks
    if ((MyXactFlags & XACT_FLAGS_ACCESSEDTEMPNAMESPACE))
        ereport(ERROR, "cannot PREPARE a transaction that has operated on temporary objects");

    if (XactHasExportedSnapshots())
        ereport(ERROR, "cannot PREPARE a transaction that has exported snapshots");

    // Phase 4: Begin preparation
    HOLD_INTERRUPTS();
    s->state = TRANS_PREPARE;

    if (TransactionTimeout > 0)
        disable_timeout(TRANSACTION_TIMEOUT, false);

    prepared_at = GetCurrentTimestamp();
    gxact = MarkAsPreparing(xid, prepareGID, prepared_at, GetUserId(), MyDatabaseId);

    // Phase 5: Collect state for 2PC file
    StartPrepare(gxact);
    AtPrepare_Notify();
    AtPrepare_Locks();
    AtPrepare_PredicateLocks();
    AtPrepare_PgStat();
    AtPrepare_MultiXact();
    AtPrepare_RelationMap();
    EndPrepare(gxact);  // Write 2PC state file

    // Phase 6: Transfer resources and detach from backend
    PostPrepare_Locks(xid);  // Transfer locks to dummy PGPROC
    ProcArrayClearTransaction(MyProc);  // Remove from process array

    CallXactCallbacks(XACT_EVENT_PREPARE);

    // Release resources in proper order
    ResourceOwnerRelease(TopTransactionResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
    AtEOXact_Buffers(true);
    AtEOXact_RelationCache(true);

    // Post-prepare cleanup for various subsystems
    PostPrepare_PgStat();
    PostPrepare_Inval();
    PostPrepare_smgr();
    PostPrepare_MultiXact(xid);
    PostPrepare_PredicateLocks(xid);

    ResourceOwnerRelease(TopTransactionResourceOwner, RESOURCE_RELEASE_LOCKS, true, true);
    ResourceOwnerRelease(TopTransactionResourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);

    PostPrepare_Twophase();  // Complete transaction detachment

    // Phase 7: Final cleanup - reset transaction state
    AtEOXact_GUC(true, 1);
    AtEOXact_SPI(true);
    AtEOXact_Enum();
    AtEOXact_on_commit_actions(true);
    // ... additional cleanup calls ...

    // Reset transaction state variables
    s->fullTransactionId = InvalidFullTransactionId;
    s->subTransactionId = InvalidSubTransactionId;
    s->nestingLevel = 0;
    s->state = TRANS_DEFAULT;

    RESUME_INTERRUPTS();
}
```

Key simplifications made:
- Condensed extensive comments into phase-based organization
- Removed detailed error handling and validation explanations
- Simplified the resource cleanup section while maintaining the critical order
- Consolidated multiple AtEOXact_* calls into representative examples
- Abstracted low-level memory and state management details
- Preserved the essential 7-phase structure: validation, pre-commit, cleanup, validation, preparation, detachment, reset