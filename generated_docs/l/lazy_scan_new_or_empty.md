# lazy_scan_new_or_empty

## Location
[src/backend/access/heap/vacuumlazy.c:1285-1388](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/vacuumlazy.c#L1285-L1388)

## Overview
lazy_scan_new_or_empty handles new and empty page processing during vacuum operations, managing FSM updates and visibility map maintenance for these special page types.

## Definition

```c
static bool
lazy_scan_new_or_empty(LVRelState *vacrel, Buffer buf, BlockNumber blkno,
					   Page page, bool sharelock, Buffer vmbuffer)
```
## Detailed Description
lazy_scan_new_or_empty provides specialized handling for new and empty heap pages that require different treatment from normal pages during vacuum operations:

**New Page Handling**:
- Detects all-zeroes pages left over from crashes during relation extension
- Records available free space in the FSM to make pages reusable
- Does not mark pages in the visibility map to ensure standby servers can discover the space
- Handles both single-page extensions and bulk extension scenarios

**Empty Page Handling**:
- Processes pages with no allocated tuples (not even LP_UNUSED items)
- Escalates from shared to exclusive lock when necessary for safe processing
- Marks empty pages as all-visible and all-frozen in both page header and visibility map
- Handles WAL logging requirements to prevent recovery issues
- Updates FSM with accurate free space information

**Lock Management**:
- Handles both shared lock and cleanup lock scenarios
- Automatically releases locks and buffers when processing is complete
- Ensures proper lock escalation for empty page modifications

## Parameters / Member Variables
- : LVRelState containing vacuum state and relation information
- : Buffer containing the page to process
- : Block number of the page being processed
- : Page pointer for direct page access
- : Boolean indicating if caller holds only shared lock (vs cleanup lock)
- : Buffer for visibility map access

## Dependencies
- Functions called/Symbols referenced:
  - [PageIsNew](../P/PageIsNew.md) / PageIsEmpty (page state detection)
  - GetRecordedFreeSpace / RecordPageWithFreeSpace (FSM management)
  - [PageIsAllVisible](../P/PageIsAllVisible.md) / PageSetAllVisible (visibility state management)
  - [visibilitymap_set](../v/visibilitymap_set.md) (visibility map updates)
  - [log_newpage_buffer](log_newpage_buffer.md) (WAL logging)
  - [PageGetHeapFreeSpace](../P/PageGetHeapFreeSpace.md) (free space calculation)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md) (buffer management)

- Called from (representative examples):
  - [lazy_scan_heap](lazy_scan_heap.md) (src/backend/access/heap/vacuumlazy.c:935)

## Notes and Other Information
- Returns true when page processing is complete (caller should continue to next page)
- Returns false when page requires normal processing via lazy_scan_prune or lazy_scan_noprune
- Handles corner cases from hard crashes during relation extension
- Critical section protection ensures atomic visibility map and page header updates
- Maintains crash safety by ensuring proper WAL logging sequence
- Does not enter new pages into visibility map to support promoted standby discovery
- Source location: src/backend/access/heap/vacuumlazy.c:1285-1388