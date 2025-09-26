# gistxlogPageDelete

## Location
[src/include/access/gistxlog.h:86-91](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/gistxlog.h#L86-L91)

## Overview
The  structure represents a WAL record for GiST index page deletion operations, capturing information needed to replay page deletions during recovery.

## Definition

```c
typedef struct gistxlogPageDelete
{
	FullTransactionId deleteXid;	/* last Xid which could see page in scan */
	OffsetNumber downlinkOffset;	/* Offset of downlink referencing this
									 * page */
} gistxlogPageDelete;
```
## Detailed Description
This structure is used to log GiST index page deletion operations in the write-ahead log. Page deletion occurs when a GiST page becomes empty and can be removed from the index structure. The structure contains the transaction ID of the last transaction that could see the page during a scan (for MVCC purposes) and the offset of the downlink that references the deleted page in its parent.

## Parameters / Member Variables
- `deleteXid`: Full transaction ID of the last transaction that could potentially see this page during an index scan, used for proper MVCC visibility handling
- `downlinkOffset`: Offset number in the parent page where the downlink (pointer) to this deleted page is located
## Dependencies
- Functions called/Symbols referenced:
  - [FullTransactionId](../F/FullTransactionId.md)
- Called from (representative examples):
  - [gistRedoPageDelete](gistRedoPageDelete.md)
  - [gistXLogPageDelete](gistXLogPageDelete.md)
  - [out_gistxlogPageDelete](../o/out_gistxlogPageDelete.md)
  - [gist_desc](gist_desc.md)
  - SizeOfGistxlogPageDelete

## Notes and Other Information
- Backup block 0 contains the page that was deleted
- Backup block 1 contains the parent page with the downlink to the deleted page
- Page deletion in GiST indexes requires careful MVCC handling to ensure that concurrent scans don't access deleted pages
- The deleteXid is crucial for determining when it's safe to reuse the deleted page
- Page deletion is typically part of a larger reorganization process in GiST indexes