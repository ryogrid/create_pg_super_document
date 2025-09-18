# gistvacuumpage

## Location
src/backend/access/gist/gistvacuum.c: 272 - 460

## Overview
Processes a single page during GiST index vacuum operations, handling tuple deletion, page recycling, split detection, and maintaining vacuum state.

## Definition


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
  - vacuum_delay_point (vacuum throttling)
  - ReadBufferExtended, LockBuffer, BufferGetPage, UnlockReleaseBuffer (buffer management)
  - gistPageRecyclable, GistPageIsDeleted, GistPageIsLeaf (page state checking)
  - RecordFreeIndexPage (FSM management)
  - GistFollowRight, GistPageGetNSN (split detection logic)
  - PageGetMaxOffsetNumber, PageGetItemId, PageGetItem (page/tuple access)
  - PageIndexMultiDelete, GistMarkTuplesDeleted (tuple deletion)
  - gistXLogUpdate, gistGetFakeLSN (WAL logging)
  - intset_add_member (page set tracking for internal and empty pages)
  - GistTupleIsInvalid (legacy tuple validation)
- Called from (representative examples):
  - gistvacuumscan (main vacuum scanning loop)

## Notes and Other Information
- Uses tail recursion optimization (implemented as a goto loop) to handle concurrent page splits efficiently
- Implements aggressive exclusive locking strategy since processing time per page is expected to be short
- Generates single WAL record per page for all tuple deletions to minimize WAL traffic
- Maintains separate tracking of internal pages and empty leaf pages using integer sets
- Handles page splits that occurred during vacuum by checking NSN (Node Sequence Number) and rightlink pointers
- Detects and reports legacy invalid tuples from pre-9.1 PostgreSQL versions with detailed diagnostic information
- Only adds pages to tracking sets when blkno == orig_blkno to maintain ascending order requirement for IntegerSet
- The function is static (internal to gistvacuum.c) and serves as the core page-processing routine