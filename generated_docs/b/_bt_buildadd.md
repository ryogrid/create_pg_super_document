# _bt_buildadd

## Location
[src/backend/access/nbtree/nbtsort.c:784-1028](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtsort.c#L784-L1028)

## Overview
A core function that adds an item to a disk page during B-tree index construction, handling page splits, high key management, and proper page layout according to B-tree conventions.

## Definition

```c
static void
_bt_buildadd(BTWriteState *wstate, BTPageState *state, IndexTuple itup,
			 Size truncextra)
```
## Detailed Description
This function is responsible for adding items to pages during B-tree index building from sorted output. It implements the complex logic required to maintain proper B-tree page layout conventions while efficiently building the index structure.

The function handles several critical aspects of B-tree construction:

1. **Page Layout Management**: Ensures proper layout conventions where rightmost pages start data items at P_HIKEY instead of P_FIRSTKEY, and on non-leaf pages, the key portion of the first item need not be stored.

2. **Page Splitting Logic**: When a page becomes full (either due to hard size limits or soft fillfactor limits), it creates a new page, properly distributes items, and establishes parent-child relationships.

3. **High Key Management**: For leaf pages, implements suffix truncation to create optimized high keys by calling , which can significantly reduce storage requirements.

4. **Tree Level Management**: Automatically creates new B-tree levels when needed by establishing parent pages and maintaining the tree structure.

5. **Sibling Link Management**: Properly sets up the doubly-linked list structure between sibling pages at each level.

The function contains detailed logic for handling posting lists and considers their impact on page space calculations, particularly important for the soft fillfactor limit.

## Parameters / Member Variables
- : BTWriteState structure containing the overall state of the index building operation
- : BTPageState structure containing the state for the current page being built
- : The IndexTuple to be added to the current page
- : Size of any posting list in the tuple, used for space calculations and truncation decisions

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS
  - PageGetFreeSpace
  - IndexTupleSize
  - MAXALIGN
  - BTMaxItemSize
  - _bt_check_third_page
  - _bt_blnewpage
  - PageGetItemId
  - PageGetItem
  - _bt_sortaddtup
  - ItemIdGetLength
  - ItemIdSetUnused
  - _bt_truncate
  - PageIndexTupleOverwrite
  - _bt_pagestate
  - BTreeTupleGetNAtts
  - BTreeTupleSetDownLink
  - CopyIndexTuple
  - BTPageGetOpaque
  - _bt_blwritepage
  - OffsetNumberNext
  - palloc0
  - BTreeTupleSetNAtts
- Called from (representative examples):
  - _bt_buildadd (recursive call for parent pages)
  - _bt_sort_dedup_finish_pending
  - _bt_uppershutdown
  - _bt_load

## Notes and Other Information
- This function implements a recursive algorithm where page splits can trigger additional calls to handle parent page updates
- The function carefully manages memory allocation and deallocation, particularly for truncated high keys
- Space calculations consider both hard limits (maximum tuple size) and soft limits (fillfactor)
- Leaf pages receive special treatment for suffix truncation to optimize storage efficiency
- The function ensures that pages maintain the minimum required number of items (at least 2 non-pivot tuples plus a high key)
- Page splits preserve the B-tree invariant that all items on a page fall within the range defined by the low and high keys
- The truncextra parameter optimization helps make better decisions about when to finish pages based on potential space savings from truncating posting lists