# xl_btree_reuse_page

## Location
src/include/access/nbtxlog.h: 186 - 193

## Overview
The xl_btree_reuse_page structure represents a WAL record for B-tree page reuse operations, primarily used to generate conflict points for Hot Standby scenarios.

## Definition
```c
typedef struct xl_btree_reuse_page
{
    RelFileLocator locator;
    BlockNumber block;
    FullTransactionId snapshotConflictHorizon;
    bool        isCatalogRel;   /* to handle recovery conflict during logical
                                 * decoding on standby */
} xl_btree_reuse_page;
```

## Detailed Description
This structure logs when a B-tree page is being reused, which is important for maintaining consistency in Hot Standby and logical replication scenarios. The primary purpose is not to log the actual page modification, but rather to establish a conflict point that ensures proper transaction isolation when pages are recycled.

When a page is reused, any transactions that might have been reading the old contents of that page need to be made aware of this change. The record includes a snapshot conflict horizon that defines which transactions might be affected, enabling proper conflict resolution in standby servers.

## Parameters / Member Variables
- `locator`: RelFileLocator identifying the specific relation file, required because the buffer is not registered with the record
- `block`: The block number of the page being reused
- `snapshotConflictHorizon`: FullTransactionId that defines the conflict horizon for snapshot consistency
- `isCatalogRel`: Boolean flag indicating if this is a catalog relation, needed for handling recovery conflicts during logical decoding on standby

## Dependencies
- Functions called/Symbols referenced:
  - [RelFileLocator](../R/RelFileLocator.md) (type)
  - BlockNumber (type)
  - FullTransactionId (type)
  - [bool](../b/bool.md) (type)

- Called from (representative examples):
  - [_bt_allocbuf](../b/_bt_allocbuf.md) (src/backend/access/nbtree/nbtpage.c:935)
  - [btree_xlog_reuse_page](../b/btree_xlog_reuse_page.md) (src/backend/access/nbtree/nbtxlog.c:1005)
  - [btree_desc](../b/btree_desc.md) (src/backend/access/rmgrdesc/nbtdesc.c:115)
  - SizeOfBtreeReusePage (src/include/access/nbtxlog.h:195)

## Notes and Other Information
- Primary purpose is conflict generation for Hot Standby, not logging actual page changes
- Must include RelFileLocator since the buffer is not registered with the WAL record
- Critical for maintaining MVCC (Multi-Version Concurrency Control) consistency during page reuse
- The isCatalogRel flag is specifically important for logical decoding scenarios on standby servers
- Page reuse conflicts ensure that concurrent transactions don't see inconsistent views of recycled pages
- This record type is essential for replication scenarios where standby servers need to maintain proper transaction isolation