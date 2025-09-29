# CommitSubTransaction

## Location
[src/backend/access/transam/xact.c:5048-5161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L5048-L5161)

## Overview
CommitSubTransaction commits a subtransaction by performing cleanup operations and transferring resources to its parent transaction, ensuring that all subtransaction changes are made permanent while maintaining transaction hierarchy integrity.

## Definition

```c
struction state */
	XLogResetInsertion();
```
## Detailed Description
CommitSubTransaction is a static function responsible for committing a subtransaction in PostgreSQL's transaction management system. The function performs a comprehensive cleanup and resource transfer process:

1. **State Validation**: Checks that the subtransaction is in TRANS_INPROGRESS state
2. **Pre-commit Processing**: Executes pre-commit callbacks and handles parallel operations cleanup
3. **State Transition**: Changes the subtransaction state to TRANS_COMMIT
4. **Command Counter Update**: Increments the command counter to ensure subtransaction commands are visible
5. **Resource Cleanup**: Performs extensive cleanup of various subsystems including:
   - Child XIDs management
   - Triggers, portals, large objects
   - Notifications, relation cache, invalidation
   - Storage manager, locks, and resource owners
   - GUCs, SPI, namespace, files, hash tables, and statistics
6. **Memory Management**: Restores parent transaction's memory context
7. **Transaction Pop**: Removes the current subtransaction from the transaction stack

The function ensures that all subtransaction resources are properly transferred to the parent transaction or cleaned up appropriately.

## Parameters / Member Variables
This function takes no parameters and operates on the global CurrentTransactionState.

## Dependencies
- Functions called/Symbols referenced:
  - [ShowTransactionState](../S/ShowTransactionState.md)
  - [TransStateAsString](../T/TransStateAsString.md)  
  - [CallSubXactCallbacks](CallSubXactCallbacks.md)
  - [AtEOSubXact_Parallel](../A/AtEOSubXact_Parallel.md)
  - [CommandCounterIncrement](CommandCounterIncrement.md)
  - [AtSubCommit_childXids](../A/AtSubCommit_childXids.md)
  - [AfterTriggerEndSubXact](../A/AfterTriggerEndSubXact.md)
  - [AtSubCommit_Portals](../A/AtSubCommit_Portals.md)
  - [AtEOSubXact_LargeObject](../A/AtEOSubXact_LargeObject.md)
  - [AtSubCommit_Notify](../A/AtSubCommit_Notify.md)
  - [ResourceOwnerRelease](../R/ResourceOwnerRelease.md)
  - [AtEOSubXact_RelationCache](../A/AtEOSubXact_RelationCache.md)
  - [AtEOSubXact_Inval](../A/AtEOSubXact_Inval.md)
  - [AtSubCommit_smgr](../A/AtSubCommit_smgr.md)
  - [XactLockTableDelete](../X/XactLockTableDelete.md)
  - XidFromFullTransactionId
  - [AtEOXact_GUC](../A/AtEOXact_GUC.md)
  - [AtEOSubXact_SPI](../A/AtEOSubXact_SPI.md)
  - [AtEOSubXact_on_commit_actions](../A/AtEOSubXact_on_commit_actions.md)
  - [AtEOSubXact_Namespace](../A/AtEOSubXact_Namespace.md)
  - [AtEOSubXact_Files](../A/AtEOSubXact_Files.md)
  - [AtEOSubXact_HashTables](../A/AtEOSubXact_HashTables.md)
  - [AtEOSubXact_PgStat](../A/AtEOSubXact_PgStat.md)
  - [AtSubCommit_Snapshot](../A/AtSubCommit_Snapshot.md)
  - [ResourceOwnerDelete](../R/ResourceOwnerDelete.md)
  - [AtSubCommit_Memory](../A/AtSubCommit_Memory.md)
  - [PopTransaction](../P/PopTransaction.md)
- Called from (representative examples):
  - [CommitTransactionCommandInternal](CommitTransactionCommandInternal.md)
  - [ReleaseCurrentSubTransaction](../R/ReleaseCurrentSubTransaction.md)

## Notes and Other Information
- The function includes a warning that callers must reassign CurrentTransactionState local pointers after calling this function
- Prior to version 8.4, PostgreSQL marked subcommit in clog at this point, but now this is only done as part of the atomic update of the whole transaction tree
- The function handles parallel mode cleanup with warnings about leaked resources
- Only the subtransaction XID lock is actually released; other locks are transferred to the parent resource owner
- The function restores the upper transaction's read-only state to handle cases where parent and child have different read-only settings
- Located in src/backend/access/transam/xact.c:5048-5161

## Simplified Source

```c
// Simplified version of CommitSubTransaction
static void
CommitSubTransaction(void)
{
    TransactionState s = CurrentTransactionState;

    // Validate subtransaction is in progress
    if (s->state != TRANS_INPROGRESS) {
        elog(WARNING, "CommitSubTransaction while in %s state",
             TransStateAsString(s->state));
    }

    // Pre-commit processing: callbacks and parallel cleanup
    CallSubXactCallbacks(SUBXACT_EVENT_PRE_COMMIT_SUB, s->subTransactionId,
                         s->parent->subTransactionId);
    AtEOSubXact_Parallel(true, s->subTransactionId);

    // Change state to committed
    s->state = TRANS_COMMIT;
    CommandCounterIncrement();  // Make subtransaction commands visible

    // Post-commit cleanup for various subsystems
    if (FullTransactionIdIsValid(s->fullTransactionId)) {
        AtSubCommit_childXids();
    }

    // Clean up triggers, portals, large objects, notifications
    AfterTriggerEndSubXact(true);
    AtSubCommit_Portals(s->subTransactionId, s->parent->subTransactionId,
                        s->parent->nestingLevel, s->parent->curTransactionOwner);
    AtEOSubXact_LargeObject(true, s->subTransactionId, s->parent->subTransactionId);
    AtSubCommit_Notify();

    // Post-commit callbacks
    CallSubXactCallbacks(SUBXACT_EVENT_COMMIT_SUB, s->subTransactionId,
                         s->parent->subTransactionId);

    // Release resources in stages
    ResourceOwnerRelease(s->curTransactionOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, false);
    AtEOSubXact_RelationCache(true, s->subTransactionId, s->parent->subTransactionId);
    AtEOSubXact_Inval(true);
    AtSubCommit_smgr();

    // Release subtransaction XID lock
    if (FullTransactionIdIsValid(s->fullTransactionId)) {
        XactLockTableDelete(XidFromFullTransactionId(s->fullTransactionId));
    }

    // Transfer remaining locks to parent and finish resource cleanup
    ResourceOwnerRelease(s->curTransactionOwner, RESOURCE_RELEASE_LOCKS, true, false);
    ResourceOwnerRelease(s->curTransactionOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, false);

    // Clean up remaining subsystems
    AtEOXact_GUC(true, s->gucNestLevel);
    AtEOSubXact_SPI(true, s->subTransactionId);
    AtEOSubXact_on_commit_actions(true, s->subTransactionId, s->parent->subTransactionId);
    AtEOSubXact_Namespace(true, s->subTransactionId, s->parent->subTransactionId);
    AtEOSubXact_Files(true, s->subTransactionId, s->parent->subTransactionId);
    AtEOSubXact_HashTables(true, s->nestingLevel);
    AtEOSubXact_PgStat(true, s->nestingLevel);
    AtSubCommit_Snapshot(s->nestingLevel);

    // Restore parent transaction state
    XactReadOnly = s->prevXactReadOnly;
    CurrentResourceOwner = s->parent->curTransactionOwner;
    CurTransactionResourceOwner = s->parent->curTransactionOwner;

    // Clean up subtransaction memory and state
    ResourceOwnerDelete(s->curTransactionOwner);
    s->curTransactionOwner = NULL;
    AtSubCommit_Memory();
    s->state = TRANS_DEFAULT;

    // Remove subtransaction from stack
    PopTransaction();
}
```

Key simplifications made:
- Removed debugging output (ShowTransactionState)
- Simplified parallel mode level checking (removed detailed warning)
- Consolidated resource release calls with explanatory comments
- Removed detailed comments about version history (8.4 changes)
- Grouped related cleanup operations together
- Added high-level comments explaining each major phase
- Preserved all essential logic flow and function calls
- Maintained proper error handling for state validation