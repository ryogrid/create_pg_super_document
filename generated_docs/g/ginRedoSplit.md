# ginRedoSplit

## Location
[src/backend/access/gin/ginxlog.c:402-439](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/ginxlog.c#L402-L439)

## Overview
Replays GIN index page split operations during WAL recovery, restoring the split pages from full-page images.

## Definition

```c
static void
ginRedoSplit(XLogReaderState *record)
```
## Detailed Description
ginRedoSplit is a WAL recovery function that replays GIN (Generalized Inverted Index) page split operations from transaction log records. Page splits occur when a GIN index page becomes too full and needs to be divided into two pages. This function handles the recovery of such splits by restoring the affected pages from full-page images stored in the WAL record.

Key functionality includes:
- Processing incomplete split completion for non-leaf pages 
- Restoring left and right pages from full-page images in the WAL record
- Handling root page splits when the split creates a new root page
- Ensuring all required full-page images are present for proper recovery

The function expects the WAL record to contain full-page images rather than incremental changes, as page splits involve significant structural modifications that are best captured as complete page snapshots.

## Parameters / Member Variables
- `*record`: XLogReaderState pointer containing the WAL record being replayed, including split operation data and full-page images
## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetData
  - [ginRedoClearIncompleteSplit](ginRedoClearIncompleteSplit.md)
  - [XLogReadBufferForRedo](../X/XLogReadBufferForRedo.md)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md)
  - elog

- Constants/Flags used:
  - GIN_INSERT_ISLEAF
  - GIN_SPLIT_ROOT
  - BLK_RESTORED

- Called from:
  - [gin_redo](gin_redo.md)

## Notes and Other Information
- Uses ginxlogSplit structure to access WAL record metadata
- Requires full-page images for all involved pages (left, right, and optionally root)
- Errors out if expected full-page images are not found in the WAL record
- For non-leaf splits, clears incomplete-split flags on child pages using block reference 3
- Root splits involve three pages: original (becomes left), new right page, and new root page
- Part of PostgreSQL's GIN index WAL recovery mechanism ensuring index structure consistency

## Simplified Source

```c
static void
ginRedoSplit(XLogReaderState *record)
{
    ginxlogSplit *data = (ginxlogSplit *) XLogRecGetData(record);
    Buffer lbuffer, rbuffer, rootbuf;
    bool isLeaf = (data->flags & GIN_INSERT_ISLEAF) != 0;
    bool isRoot = (data->flags & GIN_SPLIT_ROOT) != 0;

    // For non-leaf splits, clear incomplete split flag on child page
    if (!isLeaf)
        ginRedoClearIncompleteSplit(record, 3);

    // Restore left page from full-page image
    if (XLogReadBufferForRedo(record, 0, &lbuffer) != BLK_RESTORED)
        elog(ERROR, "GIN split record did not contain full-page image of left page");

    // Restore right page from full-page image
    if (XLogReadBufferForRedo(record, 1, &rbuffer) != BLK_RESTORED)
        elog(ERROR, "GIN split record did not contain full-page image of right page");

    // For root splits, also restore the new root page
    if (isRoot) {
        if (XLogReadBufferForRedo(record, 2, &rootbuf) != BLK_RESTORED)
            elog(ERROR, "GIN split record did not contain full-page image of root page");
        UnlockReleaseBuffer(rootbuf);
    }

    // Release all buffers
    UnlockReleaseBuffer(rbuffer);
    UnlockReleaseBuffer(lbuffer);
}
```