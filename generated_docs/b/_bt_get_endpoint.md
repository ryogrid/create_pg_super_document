# _bt_get_endpoint

## Location
src/backend/access/nbtree/nbtsearch.c: 2492 - 2572

## Overview
Finds the first or last page at a specified level in a B-tree index, providing the foundation for endpoint-based tree operations and traversals.

## Definition


## Detailed Description
This function locates either the leftmost or rightmost page at a specified level within a B-tree index structure. It implements a top-down traversal strategy, starting from either the fast root (for leaf level operations) or the true root (for internal level operations). The function handles various edge cases including deleted pages, page splits, and index corruption scenarios.

The algorithm ensures that only live pages are returned by stepping right when encountering deleted or ignored pages. For rightmost searches, it continues stepping right until reaching the actual rightmost page, accounting for concurrent page splits. The function performs level validation and provides appropriate error handling for corrupted index structures.

## Parameters / Member Variables
- : Relation - The B-tree index relation to search within
- : uint32 - The tree level to search (0 for leaf level, higher numbers for internal levels)
- : bool - If true, finds the rightmost page; if false, finds the leftmost page

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_getroot](_bt_getroot.md)
  - [_bt_gettrueroot](_bt_gettrueroot.md)
  - [_bt_relandgetbuf](_bt_relandgetbuf.md)
  - BTPageGetOpaque
  - P_IGNORE
  - P_RIGHTMOST
  - P_FIRSTDATAKEY
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [BTreeTupleGetDownLink](../B/BTreeTupleGetDownLink.md)
  - BTPageOpaque (type)
  - BT_READ (constant)
  - P_NONE (constant)
- Called from (representative examples):
  - [_bt_insert_parent](_bt_insert_parent.md)
  - [_bt_endpoint](_bt_endpoint.md)

## Notes and Other Information
- Returns InvalidBuffer if the index is empty, otherwise always returns a valid, live page
- The returned buffer is pinned and read-locked
- Uses fast root for leaf-level searches and true root for internal levels for optimization
- Implements robust error checking for index corruption with detailed error messages
- Handles concurrent operations gracefully by stepping right when necessary
- For leaf level (level 0), descends to the leftmost or rightmost child at each internal level
- Provides the foundation for other endpoint-related operations in the B-tree implementation
- This is not a static function, making it accessible from other source files in the B-tree subsystem