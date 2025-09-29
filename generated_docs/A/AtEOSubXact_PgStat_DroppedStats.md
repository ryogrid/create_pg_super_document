# AtEOSubXact_PgStat_DroppedStats

## Location
[src/backend/utils/activity/pgstat_xact.c:135-188](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_xact.c#L135-L188)

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
  - [dclist_count](../d/dclist_count.md)
  - [pgstat_get_xact_stack_level](../p/pgstat_get_xact_stack_level.md)
  - dclist_foreach_modify
  - dclist_container
  - [dclist_delete_from](../d/dclist_delete_from.md)
  - [dclist_push_tail](../d/dclist_push_tail.md)
  - [pgstat_drop_entry](../p/pgstat_drop_entry.md)
  - [pgstat_request_entry_refs_gc](../p/pgstat_request_entry_refs_gc.md)
  - [pfree](../p/pfree.md)
  - [PgStat_PendingDroppedStatsItem](../P/PgStat_PendingDroppedStatsItem.md) (struct type)
  - [xl_xact_stats_item](../x/xl_xact_stats_item.md) (struct type)
  - [dlist_mutable_iter](../d/dlist_mutable_iter.md) (struct type)
- Called from (representative examples):
  - [AtEOSubXact_PgStat](AtEOSubXact_PgStat.md) (src/backend/utils/activity/pgstat_xact.c:125)

## Notes and Other Information
- This is a static function, only accessible within the pgstat_xact.c file
- Unlike top-level transactions, subtransactions must propagate certain operations to their parent
- On subtransaction commit with drop operations: pending drops are moved to the parent transaction's pending list
- On subtransaction abort with create operations: statistics objects are immediately dropped
- The function retrieves the parent transaction state using pgstat_get_xact_stack_level
- Uses doubly-linked circular lists for efficient management of pending operations
- Ensures all pending drops are processed by asserting the list is empty at completion
- Requests garbage collection if objects couldn't be freed immediately

## Simplified Source

```c
static void
AtEOSubXact_PgStat_DroppedStats(PgStat_SubXactStatus *xact_state,
                                bool isCommit, int nestDepth)
{
    PgStat_SubXactStatus *parent_xact_state;
    dlist_mutable_iter iter;
    int not_freed_count = 0;

    // Early return if no pending drops
    if (dclist_count(&xact_state->pending_drops) == 0)
        return;

    parent_xact_state = pgstat_get_xact_stack_level(nestDepth - 1);

    // Process each pending drop operation
    dclist_foreach_modify(iter, &xact_state->pending_drops)
    {
        PgStat_PendingDroppedStatsItem *pending =
            dclist_container(PgStat_PendingDroppedStatsItem, node, iter.cur);
        xl_xact_stats_item *it = &pending->item;

        dclist_delete_from(&xact_state->pending_drops, &pending->node);

        if (!isCommit && pending->is_create)
        {
            // Subtransaction abort: drop created stats objects
            if (!pgstat_drop_entry(it->kind, it->dboid, it->objoid))
                not_freed_count++;
            pfree(pending);
        }
        else if (isCommit)
        {
            // Subtransaction commit: propagate drops to parent
            dclist_push_tail(&parent_xact_state->pending_drops, &pending->node);
        }
        else
        {
            // Just free the pending item
            pfree(pending);
        }
    }

    Assert(dclist_count(&xact_state->pending_drops) == 0);
    if (not_freed_count > 0)
        pgstat_request_entry_refs_gc();
}
```