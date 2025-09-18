# _bt_rightsib_halfdeadflag

## Location
src/backend/access/nbtree/nbtpage.c: 1752 - 1801

## Overview
This function checks whether the right sibling leaf page of a target page is marked with the ISHALFDEAD flag during B-tree page deletion operations.

## Definition


## Detailed Description
_bt_rightsib_halfdeadflag is a safety check function used during B-tree page deletion to ensure that the right sibling leaf page is not currently in a half-dead state. This prevents deletion operations from proceeding when they would encounter complications due to concurrent or interrupted VACUUM operations.

The function is specifically designed for leaf pages only (unlike _bt_leftsib_splitflag which handles both leaf and internal pages). When deleting pages that are the rightmost page of their parent, the function may actually check a "right cousin" leaf page rather than a direct sibling, representing the left edge of the subtree to the right of the to-be-deleted subtree.

If the right sibling is found to be half-dead, the deletion operation is deferred until a future VACUUM operation completes the interrupted deletion process.

## Parameters
- : The relation (B-tree index) being operated on
- : Block number of the right sibling leaf page to check (must not be P_NONE)

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_getbuf](_bt_getbuf.md) (acquires buffer for the right sibling page)
  - [BufferGetPage](../B/BufferGetPage.md) (gets page from buffer)
  - BTPageGetOpaque (gets B-tree opaque data from page)
  - P_ISLEAF (verifies page is a leaf page)
  - P_ISDELETED (verifies page is not already deleted)
  - P_ISHALFDEAD (checks if page is marked half-dead)
  - [_bt_relbuf](_bt_relbuf.md) (releases buffer)
- Called from:
  - [_bt_mark_page_halfdead](_bt_mark_page_halfdead.md) (uses this check before marking a page as half-dead)

## Notes and Other Information
- Returns true if the right sibling page is half-dead (indicating deletion should be deferred)
- Returns false if the right sibling page is safe for deletion to proceed
- Includes assertions to verify the target page is a leaf page and not already deleted
- Part of the B-tree page deletion safety mechanism to handle concurrent VACUUM operations
- The function helps prevent issues where parent pages may lack pivot tuples pointing to half-dead siblings