# AfterTriggerEndSubXact

## Location
[src/backend/commands/trigger.c:5436-5532](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L5436-L5532)

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
  - [AfterTriggerShared](AfterTriggerShared.md)
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

## Simplified Source

```c
// Simplified version of AfterTriggerEndSubXact
void AfterTriggerEndSubXact(bool isCommit) {
    int my_level = GetCurrentTransactionNestLevel();
    SetConstraintState state;

    if (isCommit) {
        // Commit path: Clean up saved state since changes are preserved
        state = afterTriggers.trans_stack[my_level].state;
        if (state != NULL) {
            pfree(state);
        }
        afterTriggers.trans_stack[my_level].state = NULL;
    } else {
        // Abort path: Restore everything to pre-subtransaction state

        // Safety check - ensure trans_stack level exists
        if (my_level >= afterTriggers.maxtransdepth) {
            return;
        }

        // Restore query depth and free query storage
        int target_depth = afterTriggers.trans_stack[my_level].query_depth;
        while (afterTriggers.query_depth > target_depth) {
            if (afterTriggers.query_depth < afterTriggers.maxquerydepth) {
                AfterTriggerFreeQuery(&afterTriggers.query_stack[afterTriggers.query_depth]);
            }
            afterTriggers.query_depth--;
        }

        // Restore global event list to pre-subtransaction state
        afterTriggerRestoreEventList(&afterTriggers.events,
                                   &afterTriggers.trans_stack[my_level].events);

        // Restore constraint state if it was saved
        state = afterTriggers.trans_stack[my_level].state;
        if (state != NULL) {
            pfree(afterTriggers.state);
            afterTriggers.state = state;
        }
        afterTriggers.trans_stack[my_level].state = NULL;

        // Unmark trigger events processed during this subtransaction
        CommandId subxact_firing_id = afterTriggers.trans_stack[my_level].firing_counter;
        for_each_event_chunk(event, chunk, afterTriggers.events) {
            AfterTriggerShared evtshared = GetTriggerSharedData(event);

            if (event->ate_flags & (AFTER_TRIGGER_DONE | AFTER_TRIGGER_IN_PROGRESS)) {
                if (evtshared->ats_firing_id >= subxact_firing_id) {
                    // Unmark events from this subtransaction
                    event->ate_flags &= ~(AFTER_TRIGGER_DONE | AFTER_TRIGGER_IN_PROGRESS);
                }
            }
        }
    }
}
```

Key simplifications made:
- Removed detailed comments and kept essential logic flow clear
- Consolidated variable declarations and simplified control flow
- Abstracted complex memory operations with high-level comments
- Preserved core commit vs abort logic distinction
- Maintained safety checks and error handling for critical paths
- Simplified the event unmarking loop while preserving the algorithm