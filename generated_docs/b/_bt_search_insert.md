# _bt_search_insert

## Location
[src/backend/access/nbtree/nbtinsert.c:317-407](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtinsert.c#L317-L407)

## Overview
A specialized search wrapper for B-tree insertions that implements fastpath optimization for rightmost leaf page insertions.

## Definition


## Detailed Description
The  function is a wrapper around  specifically designed for insertion operations. It implements a critical fastpath optimization that significantly improves performance for sequential insertions by caching and reusing the rightmost leaf page of the index.

The fastpath optimization is particularly beneficial for indexes on auto-incremented fields, datetime columns, and indexes with many NULL values. When inserting successive tuples that belong on the rightmost leaf page, this optimization avoids the expensive tree traversal from root to leaf that would otherwise be required for each insertion.

The function first attempts to use the cached rightmost leaf page. If the page is suitable (still rightmost, has sufficient space, and the new tuple belongs there), it returns NULL to indicate the fastpath can be used. Otherwise, it falls back to the standard tree search algorithm.

## Parameters / Member Variables
- : The B-tree index relation being searched
- : The associated heap relation
- : Insertion state structure containing the tuple to insert and other context

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetTargetBlock: Gets cached rightmost block number
  - [ReadBuffer](../R/ReadBuffer.md): Reads the cached page into buffer pool
  - [_bt_conditionallockbuf](_bt_conditionallockbuf.md): Attempts to acquire conditional lock on buffer
  - [_bt_checkpage](_bt_checkpage.md): Validates page structure
  - [_bt_compare](_bt_compare.md): Compares scan key with page items
  - [_bt_relbuf](_bt_relbuf.md): Releases buffer lock and pin
  - [_bt_search](_bt_search.md): Performs standard tree search when fastpath unavailable
- Called from (representative examples):
  - [_bt_doinsert](_bt_doinsert.md): Main insertion routine that uses this for page location

## Notes and Other Information
- Returns NULL when fastpath optimization succeeds, indicating no descent stack needed
- Cache is invalidated when page becomes unsuitable (not rightmost, insufficient space, etc.)
- Uses conditional locking to avoid contention - gives up optimization if lock would block
- Maintains assertion that fastpath inserts should never cause page splits
- Optimization is most effective for append-only insertion patterns
- Cache validation includes checking page is rightmost, leaf, not ignored, has space, and new tuple fits after high key