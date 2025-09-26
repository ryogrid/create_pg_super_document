# lazy_vacuum_heap_rel

## Location
[src/backend/access/heap/vacuumlazy.c:2107-2194](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/vacuumlazy.c#L2107-L2194)

## Overview
Performs the second pass over the heap during the two-pass vacuum strategy, converting LP_DEAD items to LP_UNUSED and potentially truncating line pointer arrays to reclaim free space.

## Definition

```c
static void
lazy_vacuum_heap_rel(LVRelState *vacrel)
```
## Detailed Description
This function implements the heap vacuuming phase of PostgreSQL's lazy vacuum process. It operates as the second pass in a two-pass strategy where the first pass identifies dead tuples and the second pass actually removes them from heap pages. The function iterates through all dead items collected during index vacuuming, visiting each page that contains dead items and marking them as LP_UNUSED. This allows the space to be reclaimed for future use.

The function also attempts to truncate the line pointer array on each page if there are contiguous LP_UNUSED items at the end, effectively shrinking the page overhead. It maintains visibility map information and records free space statistics for each processed page.

The two-pass approach is necessary because index entries must be removed before heap tuples can be safely removed, and index processing is more efficient when done in large batches.

## Parameters / Member Variables
- : LVRelState structure containing all vacuum operation state including the relation being vacuumed, dead items collection, vacuum strategy, and progress tracking information

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_progress_update_param](../p/pgstat_progress_update_param.md)
  - [update_vacuum_error_info](../u/update_vacuum_error_info.md)  
  - [TidStoreBeginIterate](../T/TidStoreBeginIterate.md)
  - [TidStoreIterateNext](../T/TidStoreIterateNext.md)
  - [TidStoreEndIterate](../T/TidStoreEndIterate.md)
  - [vacuum_delay_point](../v/vacuum_delay_point.md)
  - [visibilitymap_pin](../v/visibilitymap_pin.md)
  - [ReadBufferExtended](../R/ReadBufferExtended.md)
  - [lazy_vacuum_heap_page](lazy_vacuum_heap_page.md)
  - [PageGetHeapFreeSpace](../P/PageGetHeapFreeSpace.md)
  - [RecordPageWithFreeSpace](../R/RecordPageWithFreeSpace.md)
  - [restore_vacuum_error_info](../r/restore_vacuum_error_info.md)
- Called from:
  - [lazy_vacuum](lazy_vacuum.md)

## Notes and Other Information
- Only executes when do_index_vacuuming and do_index_cleanup are enabled
- Requires at least one index scan to have been completed
- Updates vacuum progress reporting to PROGRESS_VACUUM_PHASE_VACUUM_HEAP
- Maintains visibility map buffer across page visits for efficiency  
- Records free space for each vacuumed page in the free space map
- Includes assertions to verify that all expected dead items are processed
- Provides debug logging of the number of dead items removed and pages processed