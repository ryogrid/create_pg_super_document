# heap_prepare_pagescan

## Location
[src/backend/access/heap/heapam.c:538-628](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L538-L628)

## Overview
heap_prepare_pagescan prepares a heap page for efficient pagemode scanning by pruning the page and collecting offsets of all visible tuples into an array for subsequent tuple retrieval operations.

## Definition

```c
void
heap_prepare_pagescan(TableScanDesc sscan)
```
## Detailed Description
This function performs essential page preparation for pagemode scanning, a PostgreSQL optimization that allows efficient tuple retrieval by pre-filtering visible tuples. The function operates in two main phases: first, it calls heap_page_prune_opt to remove dead tuples and defragment the page, then it populates the rs_vistuples array with offsets of all visible tuples. The function handles multiple optimization paths based on page visibility status and serializable conflict detection requirements, using constant folding to optimize the most common cases where all tuples are visible and serializable conflict checking is not needed.

## Parameters / Member Variables
- `sscan`: TableScanDesc cast to HeapScanDesc containing scan state and current buffer/block information

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
  - [heap_page_prune_opt](heap_page_prune_opt.md)
  - [LockBuffer](../L/LockBuffer.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageIsAllVisible](../P/PageIsAllVisible.md)
  - [CheckForSerializableConflictOutNeeded](../C/CheckForSerializableConflictOutNeeded.md)
  - [page_collect_tuples](../p/page_collect_tuples.md) (multiple optimized call paths)
- Called from (representative examples):
  - [heapgettup_pagemode](heapgettup_pagemode.md)
  - [heapam_scan_sample_next_block](heapam_scan_sample_next_block.md)
  - HeapScanIsValid

## Notes and Other Information
- Requires SO_ALLOW_PAGEMODE flag to be set in scan descriptor
- Acquires and releases BUFFER_LOCK_SHARE for tuple visibility examination
- Implements compiler optimization through constant folding by calling page_collect_tuples with literal boolean values
- Handles hot standby visibility complexities where page-level PD_ALL_VISIBLE flag cannot be fully trusted
- Uses likely() macros to hint compiler about common execution paths
- Essential component of PostgreSQL's pagemode scanning optimization that significantly improves sequential scan performance