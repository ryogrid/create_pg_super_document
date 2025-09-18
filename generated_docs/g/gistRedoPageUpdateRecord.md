# gistRedoPageUpdateRecord

## Location
[src/backend/access/gist/gistxlog.c:70-171](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistxlog.c#L70-L171)

## Overview
Replays any GiST page update operation (except page splits) during WAL recovery, handling insertion, deletion, and replacement of index tuples on a page.

## Definition
```c
static void gistRedoPageUpdateRecord(XLogReaderState *record)
```

## Detailed Description
This function handles the WAL recovery for GiST page update operations. It processes three types of operations:

1. **Single tuple replacement**: When replacing exactly one tuple with another, it uses PageIndexTupleOverwrite for consistency with gistplacetopage
2. **Multiple tuple deletion**: Removes specified tuples from the page and marks them as deleted if on a leaf page
3. **New tuple insertion**: Adds new tuples to the page, calculating appropriate offset numbers

The function also handles follow-right data fixes on child pages when block reference 1 exists in the WAL record. This ensures referential integrity during complex page operations.

Key behaviors:
- Extracts xldata from the WAL record to determine operation parameters
- Processes deletions first, then insertions
- Maintains consistency with original page operations during recovery
- Validates that the expected number of tuples were processed
- Updates page LSN and marks buffer as dirty

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record with page update information

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData (extracts WAL record data)
  - XLogReadBufferForRedo (reads buffer for redo operation)
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md) (gets block data from WAL record)
  - [PageIndexTupleOverwrite](../P/PageIndexTupleOverwrite.md) (overwrites single tuple)
  - [PageIndexMultiDelete](../P/PageIndexMultiDelete.md) (deletes multiple tuples)
  - PageAddItem (adds new items to page)
  - GistPageIsLeaf, GistMarkTuplesDeleted (leaf page handling)
  - [gistRedoClearFollowRight](gistRedoClearFollowRight.md) (clears follow-right flags)
  - Various buffer and page management functions
- Called from (representative examples):
  - [gist_redo](gist_redo.md) (main GiST WAL redo dispatcher)

## Notes and Other Information
- This is a static function only used within gistxlog.c
- Handles complex page update scenarios during WAL recovery
- Ensures consistency between normal operations and recovery operations
- Critical for maintaining GiST index integrity after crashes
- Uses assertions to validate that the expected number of tuples are processed
- The follow-right data fix must be done while holding the lock on the target page
- Part of the comprehensive GiST WAL recovery system