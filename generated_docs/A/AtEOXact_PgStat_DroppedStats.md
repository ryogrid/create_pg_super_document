# AtEOXact_PgStat_DroppedStats

## Location
[src/backend/utils/activity/pgstat_xact.c:67-111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_xact.c#L67-L111)

## Overview
Processes pending dropped statistics entries at end of transaction, either dropping stats for deleted objects on commit or for created objects on abort.

## Definition
```c
static void AtEOXact_PgStat_DroppedStats(PgStat_SubXactStatus *xact_state, bool isCommit)
```

## Detailed Description
AtEOXact_PgStat_DroppedStats manages the cleanup of statistics entries for database objects that were created or dropped during a transaction. The function implements transactional semantics for statistics management: when a transaction commits, it drops statistics for objects that were deleted during the transaction; when a transaction aborts, it drops statistics for objects that were created during the transaction (since those objects no longer exist). The function iterates through the pending_drops list maintained in the transaction state, processing each entry according to the transaction outcome.

## Parameters / Member Variables
- `xact_state`: Pointer to PgStat_SubXactStatus containing the transaction's statistics state including pending operations
- `isCommit`: Boolean indicating whether the transaction is committing (true) or aborting (false)

## Dependencies
- Functions called/Symbols referenced:
  - [dclist_count](../d/dclist_count.md)
  - dclist_foreach_modify
  - dclist_container
  - [dclist_delete_from](../d/dclist_delete_from.md)
  - [pgstat_drop_entry](../p/pgstat_drop_entry.md)
  - [pgstat_request_entry_refs_gc](../p/pgstat_request_entry_refs_gc.md)
  - [pfree](../p/pfree.md)
  - [PgStat_PendingDroppedStatsItem](../P/PgStat_PendingDroppedStatsItem.md) (struct type)
  - [xl_xact_stats_item](../x/xl_xact_stats_item.md) (struct type)
  - [dlist_mutable_iter](../d/dlist_mutable_iter.md) (struct type)
- Called from (representative examples):
  - [AtEOXact_PgStat](AtEOXact_PgStat.md) (src/backend/utils/activity/pgstat_xact.c:54)

## Notes and Other Information
- This is a static function, only accessible within the pgstat_xact.c file
- The function handles two scenarios: commit (drops stats for deleted objects) and abort (drops stats for created objects)
- Uses doubly-linked circular lists (dclist) for efficient iteration and modification of pending operations
- Tracks objects that couldn't be freed and requests garbage collection if needed
- Each pending item contains information about the object kind, database OID, object OID, and whether it was a create operation
- The function ensures transactional consistency by only processing appropriate operations based on the transaction outcome