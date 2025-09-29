# PrepareRedoRemove

## Location
[src/backend/access/transam/twophase.c:2572-2623](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L2572-L2623)

## Overview
PrepareRedoRemove removes a global transaction entry from shared memory during WAL recovery and cleans up any associated two-phase commit disk files.

## Definition

```c
void
PrepareRedoRemove(TransactionId xid, bool giveWarning)
```
## Detailed Description
PrepareRedoRemove is the cleanup counterpart to PrepareRedoAdd, responsible for removing global transaction entries from the TwoPhaseState shared memory structure during WAL replay. The function searches through the active prepared transactions array to find the entry matching the specified transaction ID, then removes both the in-memory state and any corresponding disk files if they exist. This function is typically called during recovery when processing COMMIT PREPARED or ROLLBACK PREPARED WAL records, or when cleaning up stale transaction state that was already committed or aborted. It handles both scenarios where the transaction state exists only in memory and where it was previously checkpointed to disk.

## Parameters / Member Variables
- : The transaction ID of the prepared transaction to remove from the recovery state
- : Boolean flag indicating whether to issue warnings when removing disk files (used for error reporting control)

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockHeldByMeInMode](../L/LWLockHeldByMeInMode.md)
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - [RemoveTwoPhaseFile](../R/RemoveTwoPhaseFile.md)
  - [RemoveGXact](../R/RemoveGXact.md)
- Called from (representative examples):
  - [ProcessTwoPhaseBuffer](ProcessTwoPhaseBuffer.md)
  - [xact_redo](../x/xact_redo.md)

## Notes and Other Information
The function requires exclusive access to TwoPhaseStateLock and can only be called during recovery (RecoveryInProgress must be true). It gracefully handles cases where the transaction entry doesn't exist, which is expected during normal WAL replay scenarios. When a matching entry is found, it verifies the inredo flag is set (confirming it was added during recovery) before proceeding with cleanup. The function performs both memory cleanup via RemoveGXact and optional disk cleanup via RemoveTwoPhaseFile depending on the ondisk flag. Location: src/backend/access/transam/twophase.c:2572-2623

## Simplified Source

```c
void PrepareRedoRemove(TransactionId xid, bool giveWarning)
{
    GlobalTransaction gxact = NULL;
    int i;
    bool found = false;

    Assert(LWLockHeldByMeInMode(TwoPhaseStateLock, LW_EXCLUSIVE));
    Assert(RecoveryInProgress());

    // Find the transaction in the prepared transaction array
    for (i = 0; i < TwoPhaseState->numPrepXacts; i++)
    {
        gxact = TwoPhaseState->prepXacts[i];

        if (gxact->xid == xid)
        {
            Assert(gxact->inredo);
            found = true;
            break;
        }
    }

    // Return if transaction not found (expected during WAL replay)
    if (!found)
        return;

    // Clean up files and remove transaction
    elog(DEBUG2, "removing 2PC data for transaction %u", xid);
    if (gxact->ondisk)
        RemoveTwoPhaseFile(xid, giveWarning);
    RemoveGXact(gxact);
}
```