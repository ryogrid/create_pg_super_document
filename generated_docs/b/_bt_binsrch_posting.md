# _bt_binsrch_posting

## Location
src/backend/access/nbtree/nbtsearch.c: 596 - 681

## Overview
Performs binary search within a posting list to find the appropriate position for a given tuple identifier (TID) during B-tree insertion operations.

## Definition
```c
static int _bt_binsrch_posting(BTScanInsert key, Page page, OffsetNumber offnum)
```

## Detailed Description
This is a helper routine for `_bt_binsrch_insert()` that performs binary search within a posting list tuple to locate where a caller's scan TID belongs. Posting list tuples are used in PostgreSQL B-trees to store multiple heap TIDs that share the same index key values, providing space efficiency for duplicate keys.

The function implements a standard binary search algorithm but operates specifically on posting list entries within an index tuple. It handles various edge cases including dead tuples and corrupt indexes, providing defensive checks to ensure data integrity.

## Parameters / Member Variables
- `key`: BTScanInsert structure containing the search key and target heap TID (scantid)
- `page`: The B-tree page containing the posting list tuple
- `offnum`: Offset number of the posting list tuple on the page

## Dependencies
- Functions called/Symbols referenced:
  - PageGetItemId
  - PageGetItem
  - BTreeTupleIsPosting
  - ItemIdIsDead
  - BTreeTupleGetNPosting
  - ItemPointerCompare
  - BTreeTupleGetPostingN
- Called from:
  - _bt_binsrch_insert

## Notes and Other Information
- Returns the offset into the posting list where the caller's scantid belongs
- Returns -1 as a sentinel value if the posting list tuple has the LP_DEAD bit set
- Returns 0 if the tuple is not a posting tuple (indicating potential index corruption)
- Requires that key->heapkeyspace and key->allequalimage are true
- The function assumes posting lists have at least 2 entries (Assert(high >= 2))
- Used primarily during B-tree insertion to maintain proper ordering of heap TIDs within posting lists