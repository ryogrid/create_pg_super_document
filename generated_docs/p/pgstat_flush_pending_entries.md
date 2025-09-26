# pgstat_flush_pending_entries

## Location
[src/backend/utils/activity/pgstat.c:1182-1243](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat.c#L1182-L1243)

## Overview
Flushes all pending statistics entries to shared memory by iterating through the pending list and calling kind-specific flush callbacks, handling the complexity of list modification during iteration.

## Definition
static bool pgstat_flush_pending_entries(bool nowait)

## Detailed Description
This internal function is responsible for processing all pending statistics entries in the PostgreSQL statistics collection system. It carefully iterates through the global pgStatPending doubly-linked list, calling the appropriate flush callback for each entry type. The function handles the complex scenario where flushing an entry may add new entries to the end of the list that also need processing. It maintains careful list iteration logic to avoid issues when deleting entries during traversal, using a next-pointer tracking approach. For each entry, it calls the kind-specific flush_pending_cb callback, and if the flush succeeds, it removes the entry from the pending list using pgstat_delete_pending_entry. The function only processes variable-amount statistics (not fixed_amount) and ensures all flush callbacks are valid before attempting to call them.

## Parameters / Member Variables
- `nowait`: If true, indicates that flushing should not wait for locks and may fail; if false, flushing must succeed

## Dependencies
- Functions called/Symbols referenced:
  - dlist_is_empty
  - dlist_head_node
  - dlist_container
  - dlist_has_next
  - dlist_next_node
  - pgstat_get_kind_info
  - pgstat_delete_pending_entry
  - PgStat_EntryRef
  - PgStat_HashKey
  - PgStat_Kind
  - PgStat_KindInfo
  - dlist_node
- Called from (representative examples):
  - pgstat_report_stat (src/backend/utils/activity/pgstat.c:654)

## Notes and Other Information
- Returns true if there are still pending entries that could not be flushed, false if all entries were successfully flushed
- Uses careful iteration logic to handle list modification during traversal
- Only processes variable-amount statistics (fixed_amount stats are handled differently)
- Asserts that all kind_info entries have valid flush_pending_cb callbacks
- May queue additional pending entries during processing, which will also be processed in the same call
- Part of PostgreSQL's statistics reporting infrastructure for maintaining up-to-date shared memory statistics