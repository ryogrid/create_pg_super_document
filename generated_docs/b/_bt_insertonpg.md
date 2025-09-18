# _bt_insertonpg

## Location
[src/backend/access/nbtree/nbtinsert.c:1105-1466](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtinsert.c#L1105-L1466)

## Overview
_bt_insertonpg is a recursive function that performs tuple insertion on a specific page in a B-tree index, handling posting list splits, page splits, and parent insertions as needed.

## Definition


## Detailed Description
This recursive procedure is the core insertion mechanism for B-tree indexes. It handles several complex scenarios:

1. **Posting List Splitting**: If postingoff != 0, it splits an existing posting list tuple that overlaps with the new tuple being inserted.

2. **Page Splitting**: When there's insufficient space on the target page, it calls _bt_split() to create a new page and distribute tuples between the old and new pages.

3. **Tuple Insertion**: Inserts the new tuple (which might be a result of posting list split) onto the appropriate page.

4. **Parent Management**: After a page split, it recursively calls _bt_insert_parent() to insert the appropriate child pointer in the parent page.

5. **Metadata Updates**: Updates the metapage when a root or fast root is split.

The function ensures WAL logging for crash recovery and maintains B-tree invariants throughout the insertion process. It operates with the assumption that the caller has already acquired the necessary buffer locks and handles buffer cleanup upon completion.

## Parameters / Member Variables
- : The B-tree index relation being modified
- : The heap relation that the index references  
- : BTScanInsert structure containing search/insertion key information
- : Buffer containing the target page for insertion (must be pinned and write-locked)
- : Left-sibling buffer when inserting to non-leaf page (used to clear INCOMPLETE_SPLIT flag)
- : BTStack containing parent page information for potential recursive calls
- : The IndexTuple to be inserted
- : Size of the item being inserted (MAXALIGN'd size of itup)
- : Offset number where the new item should be inserted
- : Offset within posting list for duplicate handling (0 if not splitting posting list)
- : True if inserting because we split the only page on a tree level

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_split](_bt_split.md) (for page splitting)
  - [_bt_insert_parent](_bt_insert_parent.md) (for recursive parent insertion)
  - [_bt_swap_posting](_bt_swap_posting.md) (for posting list manipulation)
  - [PageGetFreeSpace](../P/PageGetFreeSpace.md) (to check available space)
  - [XLogBeginInsert](../X/XLogBeginInsert.md), XLogRegisterData, XLogInsert (for WAL logging)
  - Various buffer and page management functions
- Called from (representative examples):
  - [_bt_doinsert](_bt_doinsert.md) (main insertion entry point)
  - [_bt_insert_parent](_bt_insert_parent.md) (recursive calls during split propagation)

## Notes and Other Information
- This is a static function within nbtinsert.c, not exposed to external modules
- The function assumes caller has completed any incomplete splits before calling
- Buffer locks are released upon function completion, regardless of success or failure
- Supports both leaf and internal page insertions with different handling logic
- Critical sections are used around page modifications to ensure atomicity
- The function includes extensive assertion checking for debugging and correctness validation
- Handles both simple insertions and complex scenarios involving posting list splits and page splits