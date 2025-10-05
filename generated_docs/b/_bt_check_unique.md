# _bt_check_unique

## Location
[src/backend/access/nbtree/nbtinsert.c:408-814](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtinsert.c#L408-L814)

## Overview
Checks for violations of unique index constraints by scanning for conflicting tuples and handling transaction wait scenarios.

## Definition

```c
static TransactionId
_bt_check_unique(Relation rel, BTInsertState insertstate, Relation heapRel,
				 IndexUniqueCheck checkUnique, bool *is_unique,
				 uint32 *speculativeToken)
```
## Detailed Description
The  function performs uniqueness constraint validation for B-tree index insertions. It scans through tuples with the same key as the tuple being inserted to detect conflicts. The function handles various scenarios including live conflicts, transactions in progress, and dead tuples.

The function supports different checking modes: full checking with error reporting (UNIQUE_CHECK_YES), existence checking without insertion (UNIQUE_CHECK_EXISTING), and partial checking that returns immediately on potential conflicts (UNIQUE_CHECK_PARTIAL). It implements sophisticated handling of concurrent transactions, including waiting for speculative insertions and other pending transactions.

A key optimization is the ability to mark dead tuples as killed when all HOT chain members are confirmed dead, helping with index cleanup. The function also handles posting list tuples by iterating through all heap TIDs within them.

## Parameters / Member Variables
- `rel`: The B-tree index relation being checked
- `insertstate`: Current insertion state containing the tuple and search context
- `heapRel`: The heap relation associated with the index
- `checkUnique`: Type of uniqueness check to perform (NO/PARTIAL/YES/EXISTING)
- `*is_unique`: Output parameter set to false if potential conflict found
- `*speculativeToken`: Output parameter for speculative insertion token when waiting needed
## Dependencies
- Functions called/Symbols referenced:
  - [_bt_binsrch_insert](_bt_binsrch_insert.md): Performs binary search to find equal tuples
  - [_bt_compare](_bt_compare.md): Compares scan keys with page items
  - [table_index_fetch_tuple_check](../t/table_index_fetch_tuple_check.md): Checks heap tuple visibility
  - [ItemPointerCompare](../I/ItemPointerCompare.md): Compares tuple identifiers
  - [BTreeTupleGetPostingN](../B/BTreeTupleGetPostingN.md): Extracts TIDs from posting list tuples
  - [CheckForSerializableConflictIn](../C/CheckForSerializableConflictIn.md): Checks for serializable conflicts
  - [BuildIndexValueDescription](../B/BuildIndexValueDescription.md): Creates human-readable key description for error reporting
- Called from (representative examples):
  - [_bt_doinsert](_bt_doinsert.md): Main insertion routine that requires uniqueness validation

## Notes and Other Information
- Returns InvalidTransactionId when no conflict, otherwise returns transaction ID to wait for
- Sets bounds_valid state in insertstate for later reuse by _bt_findinsertloc
- Treats NULLs as equal, different from default unique index semantics (caller must handle NULL values appropriately)
- Implements fastpath optimization using cached binary search bounds to avoid redundant comparisons
- Supports CREATE INDEX CONCURRENTLY by checking if inserting tuple itself became dead
- Handles posting list tuples by processing each heap TID individually
- Marks dead tuples as killed and sets BTP_HAS_GARBAGE flag for cleanup
- For UNIQUE_CHECK_PARTIAL mode, never waits for other transactions

## Simplified Source

```c
static TransactionId _bt_check_unique(Relation rel, BTInsertState insertstate, Relation heapRel,
                                     IndexUniqueCheck checkUnique, bool *is_unique,
                                     uint32 *speculativeToken) {
    IndexTuple itup = insertstate->itup;
    IndexTuple curitup = NULL;
    BTScanInsert itup_key = insertstate->itup_key;
    SnapshotData SnapshotDirty;
    OffsetNumber offset, maxoff;
    Page page;
    BTPageOpaque opaque;
    Buffer nbuf = InvalidBuffer;
    bool found = false;
    bool inposting = false;
    int curposti = 0;

    *is_unique = true;  // Assume unique until we find a duplicate
    InitDirtySnapshot(SnapshotDirty);

    page = BufferGetPage(insertstate->buf);
    opaque = BTPageGetOpaque(page);
    maxoff = PageGetMaxOffsetNumber(page);

    // Find first tuple with same key using binary search
    offset = _bt_binsrch_insert(rel, insertstate);

    // Scan through all equal tuples looking for conflicts
    for (;;) {
        if (offset <= maxoff) {
            // Skip killed items for performance
            ItemId curitemid = PageGetItemId(page, offset);
            if (!ItemIdIsDead(curitemid)) {
                ItemPointerData htid;
                bool all_dead = false;

                // Get current tuple and check if it matches our key
                if (!inposting) {
                    if (_bt_compare(rel, itup_key, page, offset) != 0)
                        break;  // Past all equal tuples

                    curitup = (IndexTuple) PageGetItem(page, curitemid);
                }

                // Extract heap TID (handle posting lists)
                if (!BTreeTupleIsPosting(curitup)) {
                    htid = curitup->t_tid;
                } else if (!inposting) {
                    inposting = true;
                    curposti = 0;
                    htid = *BTreeTupleGetPostingN(curitup, 0);
                } else {
                    htid = *BTreeTupleGetPostingN(curitup, curposti);
                }

                // Handle special case: recheck existing tuple
                if (checkUnique == UNIQUE_CHECK_EXISTING &&
                    ItemPointerCompare(&htid, &itup->t_tid) == 0) {
                    found = true;
                }
                // Check if heap tuple exists and is visible
                else if (table_index_fetch_tuple_check(heapRel, &htid, &SnapshotDirty, &all_dead)) {
                    // Found a conflict!

                    // For partial check, return immediately without waiting
                    if (checkUnique == UNIQUE_CHECK_PARTIAL) {
                        if (nbuf != InvalidBuffer) _bt_relbuf(rel, nbuf);
                        *is_unique = false;
                        return InvalidTransactionId;
                    }

                    // Check if we need to wait for another transaction
                    TransactionId xwait = TransactionIdIsValid(SnapshotDirty.xmin) ?
                                         SnapshotDirty.xmin : SnapshotDirty.xmax;

                    if (TransactionIdIsValid(xwait)) {
                        // Must wait for conflicting transaction
                        if (nbuf != InvalidBuffer) _bt_relbuf(rel, nbuf);
                        *speculativeToken = SnapshotDirty.speculativeToken;
                        insertstate->bounds_valid = false;
                        return xwait;
                    }

                    // Definite conflict - check if our tuple is still alive
                    htid = itup->t_tid;
                    if (!table_index_fetch_tuple_check(heapRel, &htid, SnapshotSelf, NULL)) {
                        break;  // Our tuple is dead, no conflict
                    }

                    // Report unique constraint violation
                    if (nbuf != InvalidBuffer) _bt_relbuf(rel, nbuf);
                    _bt_relbuf(rel, insertstate->buf);
                    insertstate->buf = InvalidBuffer;

                    ereport(ERROR, (errcode(ERRCODE_UNIQUE_VIOLATION),
                                   errmsg("duplicate key value violates unique constraint \"%s\"",
                                          RelationGetRelationName(rel))));
                }
                // Mark dead tuples as killed for cleanup
                else if (all_dead) {
                    ItemIdMarkDead(curitemid);
                    opaque->btpo_flags |= BTP_HAS_GARBAGE;
                    MarkBufferDirtyHint(insertstate->buf, true);
                }
            }
        }

        // Advance to next tuple or TID in posting list
        if (inposting && curposti < BTreeTupleGetNPosting(curitup) - 1) {
            curposti++;  // Next TID in posting list
        } else if (offset < maxoff) {
            curposti = 0;
            inposting = false;
            offset++;    // Next tuple on page
        } else {
            // Check if we need to examine next page
            if (P_RIGHTMOST(opaque)) break;
            if (_bt_compare(rel, itup_key, page, P_HIKEY) != 0) break;

            // Move to next page
            BlockNumber nblkno = opaque->btpo_next;
            nbuf = _bt_relandgetbuf(rel, nbuf, nblkno, BT_READ);
            page = BufferGetPage(nbuf);
            opaque = BTPageGetOpaque(page);

            curposti = 0;
            inposting = false;
            maxoff = PageGetMaxOffsetNumber(page);
            offset = P_FIRSTDATAKEY(opaque);
        }
    }

    // Cleanup and verify recheck found the tuple
    if (checkUnique == UNIQUE_CHECK_EXISTING && !found) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                       errmsg("failed to re-find tuple within index \"%s\"",
                              RelationGetRelationName(rel))));
    }

    if (nbuf != InvalidBuffer) _bt_relbuf(rel, nbuf);
    return InvalidTransactionId;  // No conflict found
}
```