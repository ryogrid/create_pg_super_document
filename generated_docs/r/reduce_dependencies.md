# reduce_dependencies

## Location
src/bin/pg_dump/pg_backup_archiver.c: 4903 - 4942

## Overview
This function decrements the dependency counts of items that depend on a completed TOC entry, potentially making them ready for execution, and moves newly ready items to the ready heap for processing.

## Definition
static void reduce_dependencies(ArchiveHandle *AH, TocEntry *te, binaryheap *ready_heap)

## Detailed Description
The function plays a crucial role in the parallel restore process by managing the dependency chain resolution. When a TOC entry completes execution, this function iterates through all entries that were dependent on it (stored in the revDeps array) and decrements their dependency counters. This reflects that one of their prerequisites has been satisfied.

For each dependent entry, if all conditions are met - no remaining dependencies (depCount == 0), belongs to the current restore pass, and is currently in the pending list - the entry is considered ready for execution. The function then removes it from the pending list and adds it to the ready heap, which is a priority queue that helps determine the execution order for parallel restore operations. The ready_heap parameter being NULL indicates that the caller doesn't want list memberships to be modified, allowing for dependency count updates without scheduling changes.

## Parameters / Member Variables
- : Archive handle containing the dump metadata and restore state information
- : The TOC entry that has completed and whose dependents should have their dependency counts reduced
- : Binary heap (priority queue) where newly ready entries are placed; if NULL, no scheduling changes are made

## Dependencies
- Functions called/Symbols referenced:
  - [TocEntry](../T/TocEntry.md) (struct type)
  - [binaryheap](../b/binaryheap.md) (type)
  - pg_log_debug (logging function)
  - [_tocEntryRestorePass](../t/_tocEntryRestorePass.md) (function to determine restore pass)
  - [pending_list_remove](../p/pending_list_remove.md) (function to remove from pending list)
  - [binaryheap_add](../b/binaryheap_add.md) (function to add to binary heap)
  - Assert (assertion macro)
- Called from (representative examples):
  - [restore_toc_entries_prefork](restore_toc_entries_prefork.md)
  - [restore_toc_entries_parallel](restore_toc_entries_parallel.md)
  - [mark_restore_job_done](../m/mark_restore_job_done.md)

## Notes and Other Information
- Essential for coordinating parallel restore operations by managing dependency resolution
- The function uses reverse dependency tracking (revDeps) for efficient dependency management
- Includes safeguards against double restoration through pending list membership checks
- Debug logging helps track dependency reduction progress
- The ready_heap parameter allows flexible usage - dependency counting can be updated without affecting scheduling when set to NULL
- Works in conjunction with restore pass management to ensure items are processed in the correct order