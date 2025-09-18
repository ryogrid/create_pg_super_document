# move_to_ready_heap

## Location
[src/bin/pg_dump/pg_backup_archiver.c:4519-4553](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L4519-L4553)

## Overview
Moves all immediately-ready TOC entries from the pending list to the ready heap for parallel restoration processing.

## Definition


## Detailed Description
This function scans through the pending list of TOC (Table of Contents) entries and identifies items that are ready for immediate processing. An item is considered ready if it has no remaining dependencies (depCount == 0) and belongs to the current restore pass. Ready items are removed from the pending list and added to the binary heap structure that maintains the queue of work items available for parallel workers.

The function is critical to the parallel restore mechanism as it populates the work queue that parallel workers will consume. It ensures that only items without dependencies and appropriate for the current pass are made available for execution.

## Parameters / Member Variables
- `pending_list`: Circular doubly-linked list of TOC entries that are waiting to be processed
- `ready_heap`: Binary heap data structure that stores TOC entries ready for immediate execution by parallel workers
- `pass`: The current restore pass (e.g., PRE_DATA, DATA, POST_DATA) to filter which items should be made ready

## Dependencies
- Functions called/Symbols referenced:
  - [_tocEntryRestorePass](../t/_tocEntryRestorePass.md)
  - [pending_list_remove](../p/pending_list_remove.md)
  - [binaryheap_add](../b/binaryheap_add.md)
  - [TocEntry](../T/TocEntry.md)
  - [binaryheap](../b/binaryheap.md)
  - [RestorePass](../R/RestorePass.md)
- Called from (representative examples):
  - [restore_toc_entries_parallel](../r/restore_toc_entries_parallel.md)

## Notes and Other Information
- This function operates on a circular linked list where pending_list serves as the sentinel node
- The function carefully saves the next pointer before potentially removing an entry from the list to avoid iterator invalidation
- Only items matching the current restore pass are considered, enabling phased restoration (structure, data, constraints, etc.)
- The binary heap maintains priority ordering of ready work items for optimal scheduling