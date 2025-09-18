# _bt_moveright

## Location
src/backend/access/nbtree/nbtsearch.c: 235 - 336

## Overview
This function moves right in the B-tree when necessary to handle page splits that may have occurred between reading a downlink and accessing the target page.

## Definition


## Detailed Description
_bt_moveright implements the "move right" protocol essential for B-tree concurrency control. When following a pointer to reach a page, the page may have split since the downlink was created, requiring movement to the right sibling(s). The function examines the high key on each page to determine if further rightward movement is needed.

The algorithm handles two search modes: normal search (nextkey=false) looking for the first item >= key, and nextkey search (nextkey=true) looking for the first item > key. It continues moving right until it finds a page whose high key indicates the search key belongs on that page.

When forupdate is true, the function also completes any incomplete splits encountered, which is required before allowing insertions to proceed on a page.

## Parameters / Member Variables
- : The B-tree index relation
- : The heap relation (required when forupdate is true for split completion)
- : BTScanInsert structure containing the search key and search parameters
- : Input buffer that may need to be moved right from
- : Boolean indicating whether to complete incomplete splits encountered
- : BTStack for context when completing splits (used only if forupdate is true)
- : Lock type (BT_READ or BT_WRITE) to maintain while moving

## Dependencies
- Functions called/Symbols referenced:
  - BufferGetPage
  - BTPageGetOpaque
  - P_RIGHTMOST
  - P_INCOMPLETE_SPLIT
  - P_IGNORE
  - P_HIKEY
  - BufferGetBlockNumber
  - _bt_unlockbuf
  - _bt_lockbuf
  - _bt_finish_split
  - _bt_relbuf
  - _bt_getbuf
  - _bt_compare
  - _bt_relandgetbuf
- Called from (representative examples):
  - _bt_search

## Notes and Other Information
Key aspects of the move-right protocol:
1. Uses the high key comparison to determine if rightward movement is needed
2. For nextkey=false: moves right if scan key > high key
3. For nextkey=true: moves right if scan key >= high key
4. Handles multiple consecutive splits by continuing until the correct page is found
5. When forupdate=true, upgrades locks to BT_WRITE when encountering incomplete splits
6. Maintains the same lock type (access parameter) on the final page
7. Returns an error if it falls off the rightmost edge of the index
8. The split completion logic ensures data consistency before allowing modifications