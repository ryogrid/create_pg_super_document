# AtEOSubXact_PgStat_DroppedStats

## Location
src/backend/utils/activity/pgstat_xact.c: 135 - 188

## Overview
Processes pending dropped statistics entries at subtransaction completion, handling the propagation of drop operations to parent transactions or immediate cleanup based on subtransaction outcome.

## Definition
```c
static void AtEOSubXact_PgStat_DroppedStats(PgStat_SubXactStatus *xact_state, bool isCommit, int nestDepth)
```

## Detailed Description
AtEOSubXact_PgStat_DroppedStats is the subtransaction counterpart to AtEOXact_PgStat_DroppedStats, managing statistics cleanup for nested transactions. Unlike top-level transactions, subtransactions must consider the parent transaction context. When a subtransaction aborts and had created statistics objects, those objects are immediately dropped. When a subtransaction commits and had drop operations, those operations are propagated to the parent transaction since the parent could still abort. The function handles the complex logic of nested transaction semantics for statistics management.

## Parameters / Member Variables
- `xact_state`: Pointer to PgStat_SubXactStatus containing the subtransaction's statistics state
- `isCommit`: Boolean indicating whether the subtransaction is committing (true) or aborting (false)
- `nestDepth`: Integer representing the nesting level of the subtransaction being completed

## Dependencies
- Functions called/Symbols referenced:
  - dclist_count
  - pgstat_get_xact_stack_level
  - dclist_foreach_modify
  - dclist_container
  - dclist_delete_from
  - dclist_push_tail
  - pgstat_drop_entry
  - pgstat_request_entry_refs_gc
  - pfree
  - PgStat_PendingDroppedStatsItem (struct type)
  - xl_xact_stats_item (struct type)
  - dlist_mutable_iter (struct type)
- Called from (representative examples):
  - AtEOSubXact_PgStat (src/backend/utils/activity/pgstat_xact.c:125)

## Notes and Other Information
- This is a static function, only accessible within the pgstat_xact.c file
- Unlike top-level transactions, subtransactions must propagate certain operations to their parent
- On subtransaction commit with drop operations: pending drops are moved to the parent transaction's pending list
- On subtransaction abort with create operations: statistics objects are immediately dropped
- The function retrieves the parent transaction state using pgstat_get_xact_stack_level
- Uses doubly-linked circular lists for efficient management of pending operations
- Ensures all pending drops are processed by asserting the list is empty at completion
- Requests garbage collection if objects couldn't be freed immediately