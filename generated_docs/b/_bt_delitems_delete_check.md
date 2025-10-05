# _bt_delitems_delete_check

## Location
[src/backend/access/nbtree/nbtpage.c:1513-1694](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtpage.c#L1513-L1694)

## Overview
Main entry point for single-page cleanup operations that coordinates with the tableam to determine which index tuples are safe to delete and physically removes them from a B-tree leaf page.

## Definition

```c
struct a leaf-page-wise description of what _bt_delitems_delete()
	 * needs to do to physically delete index tuples from the page.
	 *
	 * Must sort deltids array to restore leaf-page-wise order (original order
	 * before call to tableam).  This is the order that the loop expects.
	 *
	 * Note that deltids array might be a lot smaller now.  It might even have
	 * no entries at all (with bottom-up deletion caller), in which case there
	 * is nothing left to do.
	 */
	qsort(delstate->deltids, delstate->ndeltids, sizeof(TM_IndexDelete),
		  _bt_delitems_cmp);
```
## Detailed Description
This function serves as the nbtree interface to , implementing single-page cleanup by deleting a subset of index tuples whose TIDs are determined to be safe for deletion by the table access method (tableam). The function handles both simple and bottom-up index deletion strategies.

The function operates in several phases:
1. **Table consultation**: Calls  to determine which TIDs can be safely deleted
2. **Sort restoration**: Restores the deltids array to leaf-page-wise order using 
3. **Processing logic**: Analyzes each tuple to determine if it should be completely deleted or partially updated
4. **Physical deletion**: Calls  to perform the actual page modifications

For simple index deletion, the caller provides TIDs from LP_DEAD index tuples plus extra TIDs from the same leaf page that can be included without increasing distinct table blocks. For bottom-up deletion, the caller provides all TIDs from the leaf page, giving the tableam discretion over which entries to check.

The function handles both regular index tuples and posting list tuples (which contain multiple heap TIDs), determining whether to delete entire tuples or just remove specific TIDs from posting lists.

## Parameters / Member Variables
- : The B-tree index relation being modified
- : Buffer containing the leaf page to clean up (must be pinned and write-locked)
- : The heap relation corresponding to the index (used for tableam consultation)
- : TM_IndexDeleteOp structure containing deletion candidates and state information

## Dependencies
- Functions called/Symbols referenced:
  - : Tableam interface to determine deletable TIDs
  - : Checks if relation is a catalog relation
  - : Sorts deltids array using  comparator
  - : Comparator function for restoring leaf-page-wise order
  - : Performs physical deletion and update operations
  - , , : Posting list tuple utilities
  - , : Page access functions
  - , : TID comparison functions
- Called from:
  - : Bottom-up deletion pass in deduplication
  - : Simple deletion pass during insertion
  - Various progress tracking contexts

## Notes and Other Information
- Caller must provide deltids entries in leaf-page-wise order (page offset number order, TID order for posting lists)
- The function relies on the id field of deltids elements to restore original ordering after tableam sorting
- Supports both simple deletion (LP_DEAD tuples + extras) and bottom-up deletion (all page TIDs)
- Handles snapshot conflict horizon determination for WAL logging and recovery
- Efficiently processes posting list tuples by grouping operations on the same tuple
- Manages memory allocation for BTVacuumPosting structures and ensures proper cleanup
- The function may result in no deletions if the tableam determines no TIDs are safe to delete

## Simplified Source

```c
void _bt_delitems_delete_check(Relation rel, Buffer buf, Relation heapRel,
                               TM_IndexDeleteOp *delstate)
{
    Page page = BufferGetPage(buf);
    TransactionId snapshotConflictHorizon;
    OffsetNumber deletable[MaxIndexTuplesPerPage];
    BTVacuumPosting updatable[MaxIndexTuplesPerPage];
    int ndeletable = 0, nupdatable = 0;

    // Ask tableam which tuples are safe to delete
    snapshotConflictHorizon = table_index_delete_tuples(heapRel, delstate);

    // Sort deltids back to leaf-page order after tableam processing
    qsort(delstate->deltids, delstate->ndeltids, sizeof(TM_IndexDelete),
          _bt_delitems_cmp);

    if (delstate->ndeltids == 0)
        return;  // Nothing left to delete

    // Process each deletion candidate
    for (int i = 0; i < delstate->ndeltids; i++) {
        TM_IndexStatus *dstatus = delstate->status + delstate->deltids[i].id;
        OffsetNumber idxoffnum = dstatus->idxoffnum;
        IndexTuple itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, idxoffnum));

        if (!BTreeTupleIsPosting(itup)) {
            // Simple tuple - delete entirely if safe
            if (dstatus->knowndeletable)
                deletable[ndeletable++] = idxoffnum;
        } else {
            // Posting list tuple - may delete some or all TIDs
            BTVacuumPosting vacposting = NULL;
            int nitem = BTreeTupleGetNPosting(itup);

            // Check each TID in posting list against deletion candidates
            for (int p = 0; p < nitem; p++) {
                ItemPointer ptid = BTreeTupleGetPostingN(itup, p);

                // Find matching deletable TID
                if (/* TID matches and is deletable */) {
                    if (vacposting == NULL)
                        vacposting = palloc(/* appropriate size */);
                    vacposting->deletetids[vacposting->ndeletedtids++] = p;
                }
            }

            // Decide action for posting list tuple
            if (vacposting && vacposting->ndeletedtids == nitem) {
                // Delete entire tuple
                deletable[ndeletable++] = idxoffnum;
                pfree(vacposting);
            } else if (vacposting) {
                // Partial deletion - update posting list
                updatable[nupdatable++] = vacposting;
            }
        }
    }

    // Perform physical deletions and updates
    _bt_delitems_delete(rel, buf, snapshotConflictHorizon,
                        RelationIsAccessibleInLogicalDecoding(heapRel),
                        deletable, ndeletable, updatable, nupdatable);

    // Clean up allocated memory
    for (int i = 0; i < nupdatable; i++)
        pfree(updatable[i]);
}
```