# _bt_bottomupdel_finish_pending

## Location
[src/backend/access/nbtree/nbtdedup.c:648-781](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtdedup.c#L648-L781)

## Overview
Finalizes an interval during bottom-up index deletion by moving TIDs from deduplication state to deletion state and determining which entries are duplicates for the tableam delete infrastructure.

## Definition
```c
static void _bt_bottomupdel_finish_pending(Page page, BTDedupState state, TM_IndexDeleteOp *delstate)
```

## Detailed Description
This function is called during a bottom-up deletion pass when the number of TIDs in a deduplication interval is known and the interval needs to be finalized. This happens when the caller encounters a non-duplicate tuple or runs out of tuples to process from the leaf page.

The function's primary responsibility is to determine and record which entries are duplicates, providing important information to the tableam delete infrastructure. It handles two main cases:

1. **Plain index tuples**: These are marked as "promising" if they are part of a duplicate interval, following a simple rule per the tableam contract.

2. **Posting list tuples**: These require more complex handling since they can only be formed by deduplication passes or during index builds. The function uses conservative heuristics to mark at most one TID per posting list as promising, based on which table block predominates in the posting list.

The function uses heuristics that work well in practice because it only needs to give the tableam a general idea about where to look for garbage, which tends to concentrate in relatively few table blocks.

## Parameters / Member Variables
- `page`: The B-tree leaf page being processed
- `state`: The deduplication state containing accumulated TIDs and interval information
- `delstate`: The index deletion operation state where finalized deletion candidates are stored

## Dependencies
- Functions called/Symbols referenced:
  - PageGetItemId
  - PageGetItem
  - BTreeTupleIsPosting
  - ItemIdGetLength
  - BTreeTupleGetNPosting
  - _bt_posting_valid
  - BTreeTupleGetHeapTID
  - BTreeTupleGetPostingN
  - BTreeTupleGetMaxHeapTID
  - ItemPointerGetBlockNumber
- Called from:
  - _bt_bottomupdel_pass

## Notes and Other Information
- This is a static function within the nbtdedup.c module, part of PostgreSQL's B-tree deduplication system
- The function implements sophisticated heuristics for determining which entries should be marked as "promising" for deletion
- For posting list tuples, it conservatively assumes at most one affected logical row per tuple
- The promising flag helps the tableam prioritize which table blocks to examine during deletion operations
- Located at src/backend/access/nbtree/nbtdedup.c:648-781