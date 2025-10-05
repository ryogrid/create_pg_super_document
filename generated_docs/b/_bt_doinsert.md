# _bt_doinsert

## Location
[src/backend/access/nbtree/nbtinsert.c:102-316](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtinsert.c#L102-L316)

## Overview
Handles insertion of a single index tuple into a B-tree, including uniqueness checking and conflict resolution.

## Definition

```c
bool
_bt_doinsert(Relation rel, IndexTuple itup,
			 IndexUniqueCheck checkUnique, bool indexUnchanged,
			 Relation heapRel)
```
## Detailed Description
The  function is the core routine for inserting a single index tuple into a B-tree index. Called by the public interface routine , this function performs the complete insertion process including uniqueness validation, conflict detection, and actual tuple placement.

The function supports different uniqueness checking modes: it can allow duplicates (UNIQUE_CHECK_NO/UNIQUE_CHECK_PARTIAL), throw errors for duplicates (UNIQUE_CHECK_YES), or merely check for uniqueness without inserting (UNIQUE_CHECK_EXISTING). For partial uniqueness checks, it returns true if the entry is definitively unique, false if possibly non-unique.

The function implements sophisticated conflict resolution for concurrent insertions by acquiring write locks and waiting for conflicting transactions when necessary. It optimizes for NULL key values by bypassing uniqueness checks since NULL is considered unequal to all values including itself.

## Parameters / Member Variables
- `rel`: The B-tree index relation being inserted into
- `itup`: The index tuple to insert, already filled in including TID
- `checkUnique`: Uniqueness checking mode (NO/PARTIAL/YES/EXISTING)
- `indexUnchanged`: Hint indicating if tuple is from UPDATE that didn't logically change indexed value
- `heapRel`: The heap relation associated with the index
## Dependencies
- Functions called/Symbols referenced:
  - [_bt_mkscankey](_bt_mkscankey.md): Creates scan key for the tuple
  - [_bt_search_insert](_bt_search_insert.md): Finds and locks the target leaf page
  - [_bt_check_unique](_bt_check_unique.md): Performs uniqueness validation
  - [_bt_findinsertloc](_bt_findinsertloc.md): Finds exact insertion location on page
  - [_bt_insertonpg](_bt_insertonpg.md): Actually inserts the tuple onto the page
  - [_bt_relbuf](_bt_relbuf.md): Releases buffer locks
  - [_bt_freestack](_bt_freestack.md): Frees search stack
- Called from (representative examples):
  - [btinsert](btinsert.md): Main public B-tree insertion interface

## Notes and Other Information
- Implements retry logic using goto search when waiting for conflicting transactions
- Uses BTInsertStateData to track insertion state across function calls
- Optimizes NULL key handling to avoid O(N^2) behavior with many NULL duplicates
- Maintains write locks continuously from uniqueness check through insertion completion
- Returns significance only for UNIQUE_CHECK_PARTIAL mode (true=unique, false=possibly non-unique)

## Simplified Source

```c
bool _bt_doinsert(Relation rel, IndexTuple itup,
                  IndexUniqueCheck checkUnique, bool indexUnchanged,
                  Relation heapRel) {
    bool is_unique = false;
    BTInsertStateData insertstate;
    BTScanInsert itup_key;
    BTStack stack;
    bool checkingunique = (checkUnique != UNIQUE_CHECK_NO);

    // Create scan key for the tuple to insert
    itup_key = _bt_mkscankey(rel, itup);

    // Optimize for NULL keys - skip uniqueness checks since NULL != NULL
    if (checkingunique && itup_key->anynullkeys) {
        checkingunique = false;
        is_unique = true;  // NULL is always unique
    }

    // Initialize insertion state
    insertstate.itup = itup;
    insertstate.itemsz = MAXALIGN(IndexTupleSize(itup));
    insertstate.itup_key = itup_key;
    insertstate.buf = InvalidBuffer;

search:
    // Find and lock the target leaf page
    stack = _bt_search_insert(rel, heapRel, &insertstate);

    // Check for uniqueness violations if required
    if (checkingunique) {
        TransactionId xwait;
        uint32 speculativeToken;

        xwait = _bt_check_unique(rel, &insertstate, heapRel, checkUnique,
                                &is_unique, &speculativeToken);

        // If conflicting transaction found, wait and retry
        if (TransactionIdIsValid(xwait)) {
            _bt_relbuf(rel, insertstate.buf);

            if (speculativeToken)
                SpeculativeInsertionWait(xwait, speculativeToken);
            else
                XactLockTableWait(xwait, rel, &itup->t_tid, XLTW_InsertIndex);

            if (stack) _bt_freestack(stack);
            goto search;  // Retry insertion
        }
    }

    // Actually insert the tuple (unless just checking)
    if (checkUnique != UNIQUE_CHECK_EXISTING) {
        OffsetNumber newitemoff;

        // Find exact insertion location and insert
        newitemoff = _bt_findinsertloc(rel, &insertstate, checkingunique,
                                      indexUnchanged, stack, heapRel);
        _bt_insertonpg(rel, heapRel, itup_key, insertstate.buf, InvalidBuffer,
                      stack, itup, insertstate.itemsz, newitemoff,
                      insertstate.postingoff, false);
    } else {
        _bt_relbuf(rel, insertstate.buf);
    }

    // Cleanup
    if (stack) _bt_freestack(stack);
    pfree(itup_key);

    return is_unique;
}
```