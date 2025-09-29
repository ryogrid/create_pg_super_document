# pgstat_get_transactional_drops

## Location
[src/backend/utils/activity/pgstat_xact.c:270-311](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_xact.c#L270-L311)

## Overview
Extracts statistics items that need to be dropped during transaction commit or abort, used for WAL record construction to ensure stats consistency across crashes and standby servers.

## Definition
```c
int pgstat_get_transactional_drops(bool isCommit, xl_xact_stats_item **items)
```

## Detailed Description
This function retrieves the list of statistics items that need to be dropped based on the transaction outcome (commit or abort). The behavior differs depending on the transaction state:

**On Commit (isCommit = true):**
- Returns statistics for objects that were dropped during the transaction
- Filters for items where is_create = false (existing objects being dropped)

**On Abort (isCommit = false):**  
- Returns statistics for newly created objects that need cleanup
- Filters for items where is_create = true (new objects being rolled back)

The function operates by:
1. Accessing the current transaction's statistics stack (pgStatXactStack)
2. Validating transaction nesting constraints for commits
3. Allocating memory for the result array in CurrentMemoryContext
4. Iterating through pending_drops list and filtering based on isCommit and is_create flags
5. Copying matching items to the output array

The returned items are used by commit/abort and 2PC PREPARE processing to build WAL records that ensure statistics drops are replicated and survive crashes.

## Parameters / Member Variables
- `isCommit`: Boolean indicating whether this is for commit (true) or abort (false)
- `items`: Output pointer to array of xl_xact_stats_item structures that need to be dropped

## Dependencies
- Functions called/Symbols referenced:
  - [PgStat_SubXactStatus](../P/PgStat_SubXactStatus.md) (structure type)
  - [xl_xact_stats_item](../x/xl_xact_stats_item.md) (structure type)
  - [PgStat_PendingDroppedStatsItem](../P/PgStat_PendingDroppedStatsItem.md) (structure type)
  - [dlist_iter](../d/dlist_iter.md)
  - [dclist_count](../d/dclist_count.md)
  - dclist_foreach
  - dclist_container
  - [palloc](palloc.md)
  - pgStatXactStack (global variable)

- Called from (representative examples):
  - [StartPrepare](../S/StartPrepare.md) (src/backend/access/transam/twophase.c:1085, 1087)
  - [RecordTransactionCommit](../R/RecordTransactionCommit.md) (src/backend/access/transam/xact.c:1333)
  - [RecordTransactionAbort](../R/RecordTransactionAbort.md) (src/backend/access/transam/xact.c:1775)

## Notes and Other Information
- Memory is allocated in CurrentMemoryContext and must be freed by caller
- For commits, only processes top-level transactions (nest_level == 1)
- For aborts, can handle subtransaction rollbacks (which generate WAL records)
- Essential for maintaining statistics consistency in replicated and crash-recovery scenarios
- The filtering logic ensures appropriate statistics operations: drop existing objects on commit, cleanup new objects on abort
- Part of PostgreSQL's transactional statistics system and WAL logging infrastructure

## Simplified Source

```c
int pgstat_get_transactional_drops(bool isCommit, xl_xact_stats_item **items)
{
    PgStat_SubXactStatus *xact_state = pgStatXactStack;
    int nitems = 0;
    dlist_iter iter;

    // Return 0 if no transaction state exists
    if (xact_state == NULL)
        return 0;

    // Validate transaction nesting for commits
    Assert(!isCommit || xact_state->nest_level == 1);
    Assert(!isCommit || xact_state->prev == NULL);

    // Allocate memory for result array
    *items = palloc(dclist_count(&xact_state->pending_drops) * sizeof(xl_xact_stats_item));

    // Iterate through pending drops and filter based on commit/abort
    dclist_foreach(iter, &xact_state->pending_drops)
    {
        PgStat_PendingDroppedStatsItem *pending =
            dclist_container(PgStat_PendingDroppedStatsItem, node, iter.cur);

        // Filter logic:
        // - On commit: include dropped objects (is_create = false)
        // - On abort: include new objects being rolled back (is_create = true)
        if (isCommit && pending->is_create)
            continue;
        if (!isCommit && !pending->is_create)
            continue;

        // Copy matching item to result array
        Assert(nitems < dclist_count(&xact_state->pending_drops));
        (*items)[nitems++] = pending->item;
    }

    return nitems;
}
```