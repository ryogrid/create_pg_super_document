# BTDeletedPageData

## Location
[src/include/access/nbtree.h:233-236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/nbtree.h#L233-L236)

## Overview
BTDeletedPageData is a simple structure that defines the contents of a deleted B-tree page, containing transaction information necessary for safe page recycling.

## Definition

```c
typedef struct BTDeletedPageData
{
	FullTransactionId safexid;	/* See BTPageIsRecyclable() */
} BTDeletedPageData;
```
## Detailed Description
BTDeletedPageData represents the minimal contents stored in a B-tree page that has been marked as deleted. When a B-tree page is deleted, instead of immediately recycling it, PostgreSQL stores this structure in the page's tuple area to track when the page can be safely reused.

The structure contains only a transaction ID that represents the point in time when the page was deleted. This transaction ID is used by the BTPageIsRecyclable() function to determine whether all transactions that might have been accessing the page have completed, making it safe to recycle the page for new data.

This mechanism is crucial for MVCC (Multi-Version Concurrency Control) compliance in B-tree operations, ensuring that concurrent transactions don't encounter inconsistent states when pages are deleted and recycled.

## Parameters / Member Variables
- `safexid`: A full transaction ID that indicates the deletion point of the page. This is used by BTPageIsRecyclable() to determine when the page can be safely recycled for reuse
## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionId (transaction identifier type)
- Called from (representative examples):
  - [BTPageSetDeleted](BTPageSetDeleted.md) (macro to mark a page as deleted)
  - [BTPageGetDeleteXid](BTPageGetDeleteXid.md) (macro to retrieve the deletion transaction ID)

## Notes and Other Information
- This structure is stored in the tuple area of deleted pages, while BTPageOpaqueData remains in the special area
- The safexid field enables safe page recycling by tracking transaction visibility
- This replaced an older mechanism that stored 32-bit transaction IDs in the btpo_level field of BTPageOpaqueData
- The transition to 64-bit FullTransactionId values provides better transaction wraparound handling
- Pages with this structure are not immediately recyclable and must wait until the stored transaction ID is old enough
- The BTPageIsRecyclable() function uses this safexid to make recycling decisions
- This structure is essential for maintaining data consistency in concurrent B-tree operations