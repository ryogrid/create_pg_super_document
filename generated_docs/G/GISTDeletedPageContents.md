# GISTDeletedPageContents

## Location
src/include/access/gist.h: 197 - 201

## Overview
GISTDeletedPageContents is a structure stored on deleted GiST index pages to record the transaction ID of the last transaction that could have seen the page during a scan.

## Definition
```c
typedef struct GISTDeletedPageContents
{
    /* last xid which could see the page in a scan */
    FullTransactionId deleteXid;
} GISTDeletedPageContents;
```

## Detailed Description
GISTDeletedPageContents serves as a specialized data structure for managing deleted pages in GiST indexes. When a page is deleted, it cannot be immediately reused because concurrent transactions might still need to access it. This structure tracks the transaction ID that represents the boundary for when the page can be safely recycled.

Unlike normal GiST pages that use the standard page layout with line pointers and tuples, deleted pages have a simplified structure. The GISTDeletedPageContents struct is stored immediately after the standard page header, and the page's pd_lower field points to the end of this structure, allowing for future extensibility while maintaining backward compatibility.

The design allows PostgreSQL to distinguish between different versions of the deleted page format based on the pd_lower value, enabling future enhancements while preserving the ability to read older deleted pages.

## Parameters / Member Variables
- `deleteXid`: FullTransactionId - The transaction ID of the last transaction that could potentially see this page during an index scan, used to determine when the page can be safely reused

## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionId
- Called from (representative examples):
  - [GistPageSetDeleted](GistPageSetDeleted.md) (inline function that initializes deleted pages)
  - [GistPageGetDeleteXid](GistPageGetDeleteXid.md) (inline function that retrieves the delete transaction ID)

## Notes and Other Information
- The structure is stored directly after the page header instead of using the normal tuple storage layout
- The pd_lower field serves a dual purpose: it points to the end of the structure and enables format version detection for future compatibility
- The deleteXid field is essential for MVCC (Multi-Version Concurrency Control) compliance, ensuring deleted pages are not reused prematurely
- If the deleteXid field is not present in older page formats, functions default to using FirstNormalTransactionId with epoch 0
- This approach provides a clean separation between active index pages (with normal tuple layout) and deleted pages (with simplified metadata-only layout)
- The structure supports PostgreSQL's requirement that deleted index pages remain accessible to concurrent transactions until it's provably safe to recycle them