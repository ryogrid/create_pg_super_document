# gistXLogSplit

## Location
[src/backend/access/gist/gistxlog.c:495-551](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistxlog.c#L495-L551)

## Overview
Writes a WAL (Write-Ahead Logging) record for a GiST index page split operation, recording all necessary information to replay the split during recovery.

## Definition

```c
XLogRecPtr
gistXLogSplit(bool page_is_leaf,
			  SplitPageLayout *dist,
			  BlockNumber origrlink, GistNSN orignsn,
			  Buffer leftchildbuf, bool markfollowright)
```
## Detailed Description
The  function creates a comprehensive WAL record that captures all information needed to replay a GiST page split operation during crash recovery. When a GiST page becomes too full and needs to be split into multiple pages, this function logs the split operation so it can be reconstructed on standby servers or during recovery.

The function creates a  record containing metadata about the split, then registers all the new pages created during the split with their associated data. It handles both leaf and internal page splits, and can optionally include a full page image of a left child buffer when necessary for recovery consistency.

The WAL record includes the original page's right link and NSN (Next Split Number), information about whether the original page was a leaf, the number of new pages created, and whether the follow-right flag should be marked during replay.

## Parameters / Member Variables
- `page_is_leaf`: Boolean indicating whether the original page being split is a leaf page
- `*dist`: Linked list of SplitPageLayout structures describing the new pages created by the split
- `origrlink`: Block number of the original page's right link before the split
- `orignsn`: Original Next Split Number of the page being split
- `leftchildbuf`: Buffer containing left child page (may be invalid if not needed)
- `markfollowright`: Boolean indicating whether to mark the follow-right flag during replay
## Dependencies
- Functions called/Symbols referenced:
  - [XLogBeginInsert](../X/XLogBeginInsert.md)
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md)
  - [XLogRegisterData](../X/XLogRegisterData.md)
  - [XLogRegisterBufData](../X/XLogRegisterBufData.md)
  - [XLogInsert](../X/XLogInsert.md)
  - [BufferIsValid](../B/BufferIsValid.md)
  - REGBUF_STANDARD
  - REGBUF_WILL_INIT
  - XLOG_GIST_PAGE_SPLIT
  - RM_GIST_ID
- Called from (representative examples):
  - [gistplacetopage](gistplacetopage.md) (main GiST insertion/split logic)
  - Referenced in GISTPageSplitInfo structure

## Notes and Other Information
- The function must be called within a critical section after XLogEnsureRecordSpace() has been called to reserve sufficient WAL space
- The caller is responsible for ensuring adequate WAL space is available before calling this function
- The function registers multiple buffers and data chunks, so modifications to the registration logic require corresponding changes to XLogEnsureRecordSpace() calls
- The returned XLogRecPtr can be used to set LSNs on the modified pages to ensure proper ordering during recovery
- The function handles variable numbers of split pages through the linked list structure in the dist parameter

## Simplified Source

```c
XLogRecPtr gistXLogSplit(bool page_is_leaf, SplitPageLayout *dist,
                        BlockNumber origrlink, GistNSN orignsn,
                        Buffer leftchildbuf, bool markfollowright) {
    gistxlogPageSplit xlrec;
    SplitPageLayout *ptr;
    int npage = 0;

    // Count number of pages in split
    for (ptr = dist; ptr; ptr = ptr->next)
        npage++;

    // Setup WAL record metadata
    xlrec.origrlink = origrlink;
    xlrec.orignsn = orignsn;
    xlrec.origleaf = page_is_leaf;
    xlrec.npage = (uint16) npage;
    xlrec.markfollowright = markfollowright;

    XLogBeginInsert();

    // Include left child buffer if provided
    if (BufferIsValid(leftchildbuf))
        XLogRegisterBuffer(0, leftchildbuf, REGBUF_STANDARD);

    // Register the split record data
    XLogRegisterData((char *) &xlrec, sizeof(gistxlogPageSplit));

    // Register each new page and its data
    int i = 1;
    for (ptr = dist; ptr; ptr = ptr->next) {
        XLogRegisterBuffer(i, ptr->buffer, REGBUF_WILL_INIT);
        XLogRegisterBufData(i, (char *) &(ptr->block.num), sizeof(int));
        XLogRegisterBufData(i, (char *) ptr->list, ptr->lenlist);
        i++;
    }

    return XLogInsert(RM_GIST_ID, XLOG_GIST_PAGE_SPLIT);
}
```