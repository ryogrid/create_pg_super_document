# PageGetTempPageCopySpecial

## Location
src/backend/storage/page/bufpage.c: 402 - 423

## Overview
Creates a temporary page in local memory with the same special space size and contents as the given source page.

## Definition
Page PageGetTempPageCopySpecial(Page page)

## Detailed Description
This function allocates a new temporary page in local memory that mirrors the special space characteristics of the input page. The function first determines the size of the source page, allocates memory for a new page of the same size, initializes it with the same special space size, and then copies the special space content from the source page to the new temporary page. This is particularly useful for operations that need to work on a copy of a page's special space without modifying the original page.

## Parameters / Member Variables
- page: The source page from which to copy the special space size and contents

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetPageSize](PageGetPageSize.md)
  - [palloc](../p/palloc.md)
  - PageInit
  - [PageGetSpecialSize](PageGetSpecialSize.md)
  - [PageGetSpecialPointer](PageGetSpecialPointer.md)
  - memcpy
- Called from (representative examples):
  - [gistplacetopage](../g/gistplacetopage.md) (in GiST index operations)
  - [_bt_dedup_pass](../b/_bt_dedup_pass.md) (in B-tree deduplication)
  - [btree_xlog_split](../b/btree_xlog_split.md) (in B-tree WAL recovery)
  - [btree_xlog_dedup](../b/btree_xlog_dedup.md) (in B-tree deduplication WAL recovery)
  - PageIsVerified (for page verification)

## Notes and Other Information
- The returned page is allocated in local memory using palloc() and should be freed by the caller when no longer needed
- The function preserves both the size and content of the special space from the source page
- This is commonly used in index operations where temporary manipulation of page special space is required
- The function is located in src/backend/storage/page/bufpage.c:402-423