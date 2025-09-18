# PostingItem

## Location
[src/include/access/ginblock.h:188-189](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/ginblock.h#L188-L189)

## Overview
PostingItem is a data structure used in PostgreSQL's GIN (Generalized Inverted Index) implementation to represent an entry in a non-leaf posting-tree page, containing a reference to a child block and an associated key.

## Definition


## Detailed Description
PostingItem is a fundamental structure in GIN index posting trees that serves as an entry in internal (non-leaf) pages. Each PostingItem contains two essential components: a reference to a child block in the posting tree and an associated key value. The structure is designed to minimize memory usage by using BlockIdData instead of BlockNumber to avoid padding space wastage. This structure enables the hierarchical navigation through GIN posting trees, allowing efficient traversal from root to leaf pages when searching for specific item pointers.

## Parameters / Member Variables
- : BlockIdData structure that references the child block number in the posting tree, designed to minimize padding overhead
- : ItemPointerData that represents the key value associated with this posting tree entry, used for navigation and comparison during tree traversal

## Dependencies
- Functions called/Symbols referenced:
  - [BlockIdData](../B/BlockIdData.md) (for child block reference)
  - [ItemPointerData](../I/ItemPointerData.md) (for key storage)
- Called from (representative examples):
  - [dataLocateItem](../d/dataLocateItem.md) (src/backend/access/gin/gindatapage.c:257)
  - [dataFindChildPtr](../d/dataFindChildPtr.md) (src/backend/access/gin/gindatapage.c:323)
  - [GinDataPageAddPostingItem](../G/GinDataPageAddPostingItem.md) (src/backend/access/gin/gindatapage.c:380)
  - [dataSplitPageInternal](../d/dataSplitPageInternal.md) (src/backend/access/gin/gindatapage.c:1268)
  - [ginDeletePage](../g/ginDeletePage.md) (src/backend/access/gin/ginvacuum.c:173)

## Notes and Other Information
- [PostingItem](PostingItem.md) is specifically used in non-leaf pages of GIN posting trees, not in leaf pages
- The structure includes helper macros PostingItemGetBlockNumber and PostingItemSetBlockNumber for convenient access to the child block number
- Memory layout is optimized to avoid unnecessary padding by using BlockIdData instead of BlockNumber
- This structure is critical for the hierarchical organization of GIN indexes, enabling efficient range queries and bulk operations
- Used extensively in GIN data page operations, vacuum processes, and WAL logging for crash recovery