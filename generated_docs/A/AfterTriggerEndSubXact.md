# AfterTriggerEndSubXact

## Location
src/backend/commands/trigger.c: 5436 - 5532

## Overview
Handles cleanup and state restoration when a subtransaction ends, either through commit or abort, managing trigger events and constraint states appropriately.

## Definition
```c
void AfterTriggerEndSubXact(bool isCommit)
```

## Detailed Description
AfterTriggerEndSubXact manages the cleanup of after-trigger state when a subtransaction ends. The behavior differs significantly between commit and abort scenarios. For commits, it simply discards the saved state since the changes are being preserved. For aborts, it performs comprehensive rollback including restoring the event list, constraint state, query depth, and unmarking trigger events that were processed during the aborted subtransaction.

The function operates differently based on the commit/abort status:

**For commits:**
- Frees any saved constraint state (no longer needed)
- Validates query depth consistency
- Cleans up the transaction stack entry

**For aborts:**
- Restores query depth and frees query-level storage
- Restores the global deferred-event list to its pre-subtransaction state
- Restores constraint state if it was saved
- Scans and unmarks trigger events that were marked DONE or IN_PROGRESS during the subtransaction

## Parameters / Member Variables
- `isCommit`: Boolean indicating whether the subtransaction is committing (true) or aborting (false)

## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTransactionNestLevel](../G/GetCurrentTransactionNestLevel.md)
  - [AfterTriggerFreeQuery](AfterTriggerFreeQuery.md)
  - [afterTriggerRestoreEventList](../a/afterTriggerRestoreEventList.md)
  - GetTriggerSharedData
  - for_each_event_chunk (macro)
  - [pfree](../p/pfree.md)
- Types used:
  - SetConstraintState
  - [AfterTriggerEvent](AfterTriggerEvent.md)
  - [AfterTriggerEventChunk](AfterTriggerEventChunk.md)
  - AfterTriggerShared
  - CommandId
- Constants:
  - AFTER_TRIGGER_DONE
  - AFTER_TRIGGER_IN_PROGRESS
- Called from (representative examples):
  - [CommitSubTransaction](../C/CommitSubTransaction.md) (src/backend/access/transam/xact.c:5090)
  - [AbortSubTransaction](AbortSubTransaction.md) (src/backend/access/transam/xact.c:5257)

## Notes and Other Information
- Handles the case where subtransaction start failed before AfterTriggerBeginSubXact was called
- Uses firing IDs to identify events that need to be unmarked during abort
- Assumes that subtransactions include all events from child subtransactions
- Memory cleanup is carefully managed to avoid double-free errors
- [Query](../Q/Query.md)-level storage cleanup is performed only during abort scenarios