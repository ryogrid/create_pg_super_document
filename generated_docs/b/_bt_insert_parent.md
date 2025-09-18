# _bt_insert_parent

## Location
[src/backend/access/nbtree/nbtinsert.c:2099-2240](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtinsert.c#L2099-L2240)

## Overview
_bt_insert_parent completes a page split by inserting a downlink to the new right page into the appropriate parent page, handling both normal splits and root splits.

## Definition


## Detailed Description
This function is responsible for the final step of page splitting: inserting the appropriate downlink into the parent page to make the split permanent and visible to other operations. The function handles two main scenarios:

1. **Root Split Handling**: When isroot is true, it means we've split the root page itself. In this case, a completely new root level must be created using _bt_newlevel(), which creates a new root page containing downlinks to both the old and new pages.

2. **Normal Parent Insertion**: For non-root splits, the function:
   - Re-finds the parent page using _bt_getstackbuf() (since the parent location may have changed during concurrent operations)
   - Creates a new index tuple containing the high key from the left page and a downlink to the new right page
   - Recursively calls _bt_insertonpg() to insert this downlink into the parent

The function handles edge cases like concurrent root splits where the stack might be NULL, requiring reconstruction of parent information. It carefully manages buffer locks to prevent concurrent VACUUM operations from becoming confused during the split process.

## Parameters / Member Variables
- : The B-tree index relation being modified
- : The heap relation referenced by the index
- : Buffer containing the left (original) page from the split
- : Buffer containing the new right page from the split
- : BTStack containing parent page information (NULL for root splits or concurrent operations)
- : True if we split the actual root page
- : True if we split a page that was alone on its level (might have been fast root)

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_newlevel](_bt_newlevel.md) (for creating new root level)
  - [_bt_getstackbuf](_bt_getstackbuf.md) (to re-find and lock parent page)
  - [_bt_insertonpg](_bt_insertonpg.md) (recursive call to insert downlink)
  - [_bt_get_endpoint](_bt_get_endpoint.md) (to find leftmost page when stack is NULL)
  - [CopyIndexTuple](../C/CopyIndexTuple.md), BTreeTupleSetDownLink (to create parent downlink tuple)
  - Buffer management functions (_bt_relbuf)
- Called from (representative examples):
  - [_bt_insertonpg](_bt_insertonpg.md) (after page split completion)
  - [_bt_finish_split](_bt_finish_split.md) (during split completion in certain scenarios)

## Notes and Other Information
- This is a static function within nbtinsert.c, not exposed externally
- Releases both buf and rbuf buffer locks upon completion
- Handles concurrent operations gracefully by re-finding parent pages when necessary
- The function ensures atomicity by delaying the release of the right child buffer until parent insertion is ready
- For root splits, it creates an entirely new tree level and updates the metapage accordingly
- When stack is NULL due to concurrent operations, it constructs a "fake" stack to enable normal processing
- The function maintains B-tree invariants by ensuring proper downlink insertion and INCOMPLETE_SPLIT flag management
- Includes assertions to catch performance issues with fastpath optimization usage
- Error handling includes corruption detection when parent re-finding fails