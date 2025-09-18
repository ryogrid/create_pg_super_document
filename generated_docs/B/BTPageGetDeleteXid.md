# BTPageGetDeleteXid

## Location
[src/include/access/nbtree.h:260-290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L260-L290)

## Overview
BTPageGetDeleteXid is a static inline function that retrieves the safe transaction ID from a deleted B-tree page, indicating when the page can be safely recycled.

## Definition
```c
static inline FullTransactionId
BTPageGetDeleteXid(Page page)
```

## Detailed Description
This function extracts the safe transaction ID from a deleted B-tree page. It performs validation checks to ensure the page is actually deleted, handles special cases for pages that were deleted during pg_upgrade (which don't have full transaction IDs), and returns the stored safe transaction ID from the page's contents.

The function includes assertions to verify the page is deleted and not new. For pages that lack a full transaction ID (typically from older PostgreSQL versions after pg_upgrade), it returns FirstNormalFullTransactionId, indicating these pages are safe to recycle immediately.

## Parameters / Member Variables
- `page`: The deleted B-tree page from which to retrieve the safe transaction ID

## Dependencies
- Functions called/Symbols referenced:
  - [PageIsNew](../P/PageIsNew.md)
  - BTPageGetOpaque
  - P_ISDELETED
  - P_HAS_FULLXID
  - [PageGetContents](../P/PageGetContents.md)
  - BTPageOpaque (type)
  - [BTDeletedPageData](BTDeletedPageData.md) (type)
  - FirstNormalFullTransactionId (constant)
- Called from (representative examples):
  - [_bt_allocbuf](../b/_bt_allocbuf.md)
  - [BTPageIsRecyclable](BTPageIsRecyclable.md)

## Notes and Other Information
This function is essential for B-tree page recycling logic and MVCC compliance. It handles backward compatibility with older PostgreSQL versions where deleted pages didn't store full transaction IDs. The returned transaction ID is used to determine if it's safe to reuse the page by comparing it with the oldest active transaction. The function assumes the page has already been verified as deleted and includes debug assertions to catch programming errors.