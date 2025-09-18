# pop_next_work_item

## Location
[src/bin/pg_dump/pg_backup_archiver.c:4554-4610](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L4554-L4610)

## Overview
Finds and removes the next suitable work item from the ready heap that can be executed without lock conflicts.

## Definition
static TocEntry *pop_next_work_item(binaryheap *ready_heap, ParallelState *pstate)

## Detailed Description
This function searches through the ready heap to find a TOC entry that can be safely executed in parallel with currently running items. While the heap contains items known to have no remaining dependencies, this function performs additional checks for lock conflicts between the candidate item and any currently running items.

The function performs a sequential scan through heap nodes rather than strictly following heap priority order, as lock conflict checking may require skipping high-priority items in favor of lower-priority ones that don't conflict. However, it typically selects one of the first few items which usually have relatively high priority.

## Parameters / Member Variables
- ready_heap: Binary heap containing TOC entries that have no remaining dependencies and are ready for execution
- pstate: Parallel state structure containing information about currently running worker processes and their assigned TOC entries

## Dependencies
- Functions called/Symbols referenced:
  - binaryheap_size
  - binaryheap_get_node
  - [has_lock_conflicts](../h/has_lock_conflicts.md)
  - [binaryheap_remove_node](../b/binaryheap_remove_node.md)
  - pg_log_debug
  - [TocEntry](../T/TocEntry.md)
  - [binaryheap](../b/binaryheap.md)
  - [ParallelState](../P/ParallelState.md)
- Called from (representative examples):
  - [restore_toc_entries_parallel](../r/restore_toc_entries_parallel.md)

## Notes and Other Information
- Returns the selected TOC entry or NULL if no suitable item is available
- Lock conflict checking prevents concurrent execution of items that would interfere with each other
- The function removes the selected item from the heap, so it becomes unavailable for other workers
- Uses bidirectional conflict checking (te conflicts with running_te AND running_te conflicts with te)
- Logs debug information when no items are available for execution