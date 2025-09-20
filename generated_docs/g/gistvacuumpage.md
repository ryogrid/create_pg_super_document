# gistvacuumpage

## Location
[src/backend/access/gist/gistvacuum.c:272-460](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistvacuum.c#L272-L460)

## Overview
Processes a single page during GiST index vacuum operations, handling tuple deletion, page recycling, split detection, and maintaining vacuum state.

## Definition

```c
static void
gistvacuumpage(GistVacState *vstate, BlockNumber blkno, BlockNumber orig_blkno)
```
## Detailed Description
This function performs detailed processing of individual pages during GiST vacuum operations. It handles multiple scenarios: recyclable pages that can be immediately reused, deleted pages that need tracking, leaf pages requiring tuple-level processing, and internal pages that need structural validation.

For leaf pages, the function implements sophisticated logic to detect and handle concurrent page splits that might have occurred during the vacuum scan. When splits move tuples to lower-numbered pages that were already processed, the function sets up tail recursion to revisit those pages.

The function performs tuple-level deletion on leaf pages using provided callback criteria, batching deletions for efficiency and generating appropriate WAL records. It also detects completely empty pages for later removal and maintains accurate tuple counts for statistics.

For internal pages, it validates tuple integrity and detects legacy "invalid tuples" from PostgreSQL versions prior to 9.1, providing diagnostic messages when encountered.

## Parameters / Member Variables
- : GistVacState structure containing vacuum context, statistics, page sets, and callback information
- : Block number of the page currently being processed
- : Highest block number reached by the outer scan (used for split detection and recursion control)

## Dependencies
- Functions called/Symbols referenced:
  - [vacuum_delay_point](../v/vacuum_delay_point.md) (vacuum throttling)
  - [ReadBufferExtended](../R/ReadBufferExtended.md), LockBuffer, BufferGetPage, UnlockReleaseBuffer (buffer management)
  - [gistPageRecyclable](gistPageRecyclable.md), GistPageIsDeleted, GistPageIsLeaf (page state checking)
  - RecordFreeIndexPage (FSM management)
  - GistFollowRight, GistPageGetNSN (split detection logic)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md), PageGetItemId, PageGetItem (page/tuple access)
  - [PageIndexMultiDelete](../P/PageIndexMultiDelete.md), GistMarkTuplesDeleted (tuple deletion)
  - [gistXLogUpdate](gistXLogUpdate.md), gistGetFakeLSN (WAL logging)
  - [intset_add_member](../i/intset_add_member.md) (page set tracking for internal and empty pages)
  - GistTupleIsInvalid (legacy tuple validation)
- Called from (representative examples):
  - [gistvacuumscan](gistvacuumscan.md) (main vacuum scanning loop)

## Notes and Other Information
- Uses tail recursion optimization (implemented as a goto loop) to handle concurrent page splits efficiently
- Implements aggressive exclusive locking strategy since processing time per page is expected to be short
- Generates single WAL record per page for all tuple deletions to minimize WAL traffic
- Maintains separate tracking of internal pages and empty leaf pages using integer sets
- Handles page splits that occurred during vacuum by checking NSN (Node Sequence Number) and rightlink pointers
- Detects and reports legacy invalid tuples from pre-9.1 PostgreSQL versions with detailed diagnostic information
- Only adds pages to tracking sets when blkno == orig_blkno to maintain ascending order requirement for IntegerSet
- The function is static (internal to gistvacuum.c) and serves as the core page-processing routine