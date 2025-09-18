# lazy_vacuum_heap_page

## Location
[src/backend/access/heap/vacuumlazy.c:2195-2299](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/vacuumlazy.c#L2195-L2299)

## Overview
Processes a single heap page during vacuum by converting specified LP_DEAD items to LP_UNUSED, attempting line pointer array truncation, and updating visibility map information.

## Definition


## Detailed Description
This function performs the actual heap page cleanup during the vacuum process. It takes a list of dead item offsets on a specific page and converts those LP_DEAD line pointers to LP_UNUSED, making the space available for reuse. The function operates within a critical section to ensure atomicity of the page modifications.

After marking items as unused, it attempts to truncate the line pointer array if there are contiguous unused items at the end, which helps reduce page overhead. The function also logs the changes to WAL if needed and updates the visibility map if the page becomes all-visible or all-frozen after the cleanup.

The function carefully manages the critical section to avoid doing complex operations (like visibility tests) while holding exclusive locks, which could lead to deadlocks or performance issues.

## Parameters / Member Variables
- : LVRelState structure containing vacuum operation state and relation information
- : Block number of the heap page being processed  
- : Buffer containing the heap page, must be exclusively locked by caller
- : Array of offset numbers for LP_DEAD items to be marked as LP_UNUSED
- : Number of offsets in the deadoffsets array
- : Buffer for the visibility map page, must be pinned by caller

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_progress_update_param](../p/pgstat_progress_update_param.md)
  - [update_vacuum_error_info](../u/update_vacuum_error_info.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - ItemIdIsDead
  - ItemIdHasStorage  
  - ItemIdSetUnused
  - [PageTruncateLinePointerArray](../P/PageTruncateLinePointerArray.md)
  - MarkBufferDirty
  - RelationNeedsWAL
  - [log_heap_prune_and_freeze](log_heap_prune_and_freeze.md)
  - [heap_page_is_all_visible](../h/heap_page_is_all_visible.md)
  - [PageSetAllVisible](../P/PageSetAllVisible.md)
  - [visibilitymap_set](../v/visibilitymap_set.md)
  - [restore_vacuum_error_info](../r/restore_vacuum_error_info.md)
- Called from:
  - [lazy_vacuum_heap_rel](lazy_vacuum_heap_rel.md)

## Notes and Other Information
- Requires caller to hold exclusive buffer lock (cleanup lock also acceptable)
- Requires vmbuffer to be valid and pinned on the visibility map page for blkno
- Only processes pages when do_index_vacuuming is enabled
- Updates progress reporting with PROGRESS_VACUUM_HEAP_BLKS_VACUUMED
- Uses critical sections around page modifications to ensure atomicity
- Attempts to set page as all-visible/all-frozen after cleanup if conditions are met
- Logs WAL record with PRUNE_VACUUM_CLEANUP reason when relation needs WAL
- Includes assertions to verify that processed items are actually LP_DEAD without storage