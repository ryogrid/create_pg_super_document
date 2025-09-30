# log_newpage_buffer

## Location
[src/backend/access/transam/xloginsert.c:1237-1269](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xloginsert.c#L1237-L1269)

## Overview
log_newpage_buffer writes a WAL record containing a full image of a page for crash recovery, operating on a buffer and automatically extracting the page location information.

## Definition
```c
XLogRecPtr log_newpage_buffer(Buffer buffer, bool page_std)
```

## Detailed Description
This function creates a Write-Ahead Log (WAL) record that contains a complete image of a page for crash recovery purposes. It serves as a wrapper around the lower-level `log_newpage` function by automatically extracting the relation file locator, fork number, and block number from the provided buffer. The function must be called within a critical section after the caller has initialized and marked the buffer as dirty. The function will set the page LSN (Log Sequence Number) as part of the WAL logging process.

The function supports optimization for standard page layouts by allowing unused space between pd_lower and pd_upper to be excluded from the WAL record when page_std is set to true, resulting in smaller WAL records.

## Parameters / Member Variables
- `buffer`: The buffer containing the page to be logged to WAL
- `page_std`: Boolean flag indicating whether the page follows standard layout (allows optimization by excluding unused space from WAL record)

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md) (to extract the page from the buffer)
  - [BufferGetTag](../B/BufferGetTag.md) (to extract relation file locator, fork number, and block number)
  - [log_newpage](log_newpage.md) (the underlying function that creates the WAL record)
- Called from (representative examples):
  - [brinbuildempty](../b/brinbuildempty.md) (BRIN index empty page creation)
  - [brin_initialize_empty_new_buffer](../b/brin_initialize_empty_new_buffer.md) (BRIN buffer initialization)
  - [ginbuildempty](../g/ginbuildempty.md) (GIN index empty page creation)
  - [gistbuildempty](../g/gistbuildempty.md) (GiST index empty page creation)
  - [lazy_scan_new_or_empty](lazy_scan_new_or_empty.md) (vacuum operations)
  - [visibilitymap_prepare_truncate](../v/visibilitymap_prepare_truncate.md) (visibility map operations)
  - [RelationCopyStorageUsingBuffer](../R/RelationCopyStorageUsingBuffer.md) (relation storage operations)
  - [FreeSpaceMapPrepareTruncateRel](../F/FreeSpaceMapPrepareTruncateRel.md) (free space map operations)

## Notes and Other Information
- Must be called within a critical section (CritSectionCount > 0)
- The caller is responsible for initializing the buffer and marking it dirty before calling this function
- The function automatically sets the page LSN as part of the WAL logging process
- When page_std is true, unused space in standard page layouts is excluded from the WAL record for efficiency
- This is a higher-level interface compared to log_newpage, automatically handling buffer tag extraction

## Simplified Source

```c
XLogRecPtr log_newpage_buffer(Buffer buffer, bool page_std)
{
    Page page = BufferGetPage(buffer);
    RelFileLocator rlocator;
    ForkNumber forknum;
    BlockNumber blkno;

    // Must be in critical section
    Assert(CritSectionCount > 0);

    // Extract location info from buffer
    BufferGetTag(buffer, &rlocator, &forknum, &blkno);

    // Create WAL record for full page image
    return log_newpage(&rlocator, forknum, blkno, page, page_std);
}
```