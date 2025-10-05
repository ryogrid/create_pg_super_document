# PageRestoreTempPage

## Location
[src/backend/storage/page/bufpage.c:424-436](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/page/bufpage.c#L424-L436)

## Overview
Copies the contents of a temporary page back to the original permanent page and releases the temporary page memory.

## Definition
void PageRestoreTempPage(Page tempPage, Page oldPage)

## Detailed Description
This function completes the lifecycle of temporary page processing by copying all content from a temporary page back to the original permanent page and then freeing the memory used by the temporary page. It determines the page size from the temporary page and performs a complete memory copy to restore the original page with any modifications that were made to the temporary copy. This function is typically used as the final step in operations that required temporary page manipulation.

## Parameters / Member Variables
- tempPage: The temporary page containing the modified content to be copied back
- oldPage: The original permanent page that will receive the updated content

## Dependencies
- Functions called/Symbols referenced:
  - [PageGetPageSize](PageGetPageSize.md)
  - memcpy
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [createPostingTree](../c/createPostingTree.md) (in GIN index posting tree creation)
  - [ginbulkdelete](../g/ginbulkdelete.md) (in GIN index bulk deletion)
  - [gistplacetopage](../g/gistplacetopage.md) (in GiST index operations)
  - [_bt_dedup_pass](../b/_bt_dedup_pass.md) (in B-tree deduplication)
  - [_bt_split](../b/_bt_split.md) (in B-tree page splitting)
  - [btree_xlog_split](../b/btree_xlog_split.md) (in B-tree WAL recovery)
  - [btree_xlog_dedup](../b/btree_xlog_dedup.md) (in B-tree deduplication WAL recovery)
  - PageIsVerified (for page verification)

## Notes and Other Information
- This function is typically paired with PageGetTempPageCopySpecial or similar temporary page creation functions
- The temporary page memory is automatically freed, so the caller should not attempt to access tempPage after calling this function
- The complete page content is copied, including both data and special space
- This is a destructive operation that completely overwrites the original page content
- The function is located in src/backend/storage/page/bufpage.c:424-436

## Simplified Source

```c
void PageRestoreTempPage(Page tempPage, Page oldPage)
{
    Size pageSize;

    // Get the size of the temporary page
    pageSize = PageGetPageSize(tempPage);

    // Copy all content from temporary page to original page
    memcpy((char *) oldPage, (char *) tempPage, pageSize);

    // Free the temporary page memory
    pfree(tempPage);
}
```