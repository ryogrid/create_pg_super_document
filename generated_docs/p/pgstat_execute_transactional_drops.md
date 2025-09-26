# pgstat_execute_transactional_drops

## Location
[src/backend/utils/activity/pgstat_xact.c:312-331](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_xact.c#L312-L331)

## Overview
Executes scheduled statistics drops after transaction commit or abort, handling both recovery scenarios and normal two-phase commit processing.

## Definition
```c
void pgstat_execute_transactional_drops(int ndrops, struct xl_xact_stats_item *items, bool is_redo)
```

## Detailed Description
This function executes the actual statistics drops that were deferred during transaction processing. It processes an array of statistics items that need to be dropped, calling pgstat_drop_entry() for each item. The function serves multiple execution contexts:

1. **Recovery Mode**: Called from xact_redo_commit()/xact_redo_abort() during WAL replay
2. **Two-Phase Commit**: Called from FinishPreparedTransaction() during COMMIT/ABORT PREPARED processing
3. **Normal Transaction End**: Processes deferred drops after transaction completion

The function operates by:
1. Iterating through the provided array of xl_xact_stats_item structures
2. Calling pgstat_drop_entry() with the kind, dboid, and objoid from each item
3. Tracking items that couldn't be immediately freed (due to active references)
4. Requesting garbage collection if any entries couldn't be freed

This deferred execution model ensures that statistics drops are atomic with respect to the transaction and can be properly replicated and recovered.

## Parameters / Member Variables
- `ndrops`: Number of items in the drops array
- `items`: Array of xl_xact_stats_item structures representing statistics entries to drop
- `is_redo`: Boolean indicating if this is being called during WAL replay (currently unused but available for future extensions)

## Dependencies
- Functions called/Symbols referenced:
  - xl_xact_stats_item (structure type)
  - pgstat_drop_entry
  - pgstat_request_entry_refs_gc

- Called from (representative examples):
  - FinishPreparedTransaction (src/backend/access/transam/twophase.c:1609, 1611)
  - xact_redo_commit (src/backend/access/transam/xact.c:6185)
  - xact_redo_abort (src/backend/access/transam/xact.c:6296)

## Notes and Other Information
- Returns immediately if ndrops is 0 (no work to do)
- Handles memory management gracefully by tracking entries that couldn't be immediately freed
- Requests garbage collection when needed to ensure eventual memory reclamation
- Essential for maintaining statistics consistency across transaction boundaries
- Part of PostgreSQL's transactional statistics system, ensuring drops are atomic with transactions
- Used in both normal operation and recovery scenarios, providing consistent behavior
- The is_redo parameter allows for potential future differentiation between normal and recovery processing