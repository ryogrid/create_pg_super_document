# gistRedoClearFollowRight

## Location
[src/backend/access/gist/gistxlog.c:40-69](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistxlog.c#L40-L69)

## Overview
Replays the clearing of the F_FOLLOW_RIGHT flag on a child page during WAL recovery, ensuring the flag is properly updated even when a full-page image is restored.

## Definition
```c
static void gistRedoClearFollowRight(XLogReaderState *record, uint8 block_id)
```

## Detailed Description
This function handles the WAL recovery operation for clearing the F_FOLLOW_RIGHT flag on a GiST index page. The F_FOLLOW_RIGHT flag is used during page splits to indicate that a page has a right link that should be followed. This function is crucial because:

1. Even when a full-page image is restored from WAL, the follow-right flag change is not included in the image
2. It ensures the intermediate state with incorrect flag values is not visible to concurrent Hot Standby queries
3. It handles both the full-page image restoration and flag updates in a single operation
4. Updates the page's NSN (Next Sequence Number) to maintain consistency

The function reads the buffer for redo, updates the NSN, clears the follow-right flag, sets the LSN, and marks the buffer as dirty before releasing it.

## Parameters / Member Variables
- `record`: XLogReaderState pointer containing the WAL record being replayed
- `block_id`: Block identifier (uint8) specifying which block in the WAL record to process

## Dependencies
- Functions called/Symbols referenced:
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md) (reads buffer for redo operation)
  - GistPageSetNSN (sets Next Sequence Number on the page)
  - GistClearFollowRight (clears the follow-right flag)
  - [BufferGetPage](../B/BufferGetPage.md), PageSetLSN, MarkBufferDirty, UnlockReleaseBuffer (buffer management)
  - XLogRedoAction, BLK_NEEDS_REDO, BLK_RESTORED (WAL redo action types)
- Called from (representative examples):
  - [gistRedoPageUpdateRecord](gistRedoPageUpdateRecord.md)
  - [gistRedoPageSplitRecord](gistRedoPageSplitRecord.md)

## Notes and Other Information
- This is a static function only used within gistxlog.c
- Critical for maintaining GiST index consistency during WAL recovery
- Handles the special case where full-page images don't include flag updates
- Part of the GiST index WAL recovery infrastructure

## Simplified Source

```c
static void
gistRedoClearFollowRight(XLogReaderState *record, uint8 block_id)
{
    XLogRecPtr lsn = record->EndRecPtr;
    Buffer buffer;
    Page page;
    XLogRedoAction action;

    // Read buffer for redo operation
    action = XLogReadBufferForRedo(record, block_id, &buffer);

    if (action == BLK_NEEDS_REDO || action == BLK_RESTORED) {
        page = BufferGetPage(buffer);

        // Update page state
        GistPageSetNSN(page, lsn);        // Set Next Sequence Number
        GistClearFollowRight(page);       // Clear the follow-right flag

        // Complete buffer management
        PageSetLSN(page, lsn);
        MarkBufferDirty(buffer);
    }

    if (BufferIsValid(buffer))
        UnlockReleaseBuffer(buffer);
}
```
- The function ensures atomic updates to prevent race conditions in Hot Standby scenarios