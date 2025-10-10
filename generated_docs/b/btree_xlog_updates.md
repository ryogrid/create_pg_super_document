# btree_xlog_updates

## Location
[src/backend/access/nbtree/nbtxlog.c:557-597](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtxlog.c#L557-L597)

## Overview
Processes WAL record data to update posting list tuples on a B-tree page by removing specified heap TIDs during recovery.

## Definition

```c
static void
btree_xlog_updates(Page page, OffsetNumber *updatedoffsets,
				   xl_btree_update *updates, int nupdated)
```
## Detailed Description
This function applies updates to posting list tuples on a B-tree page during WAL recovery. It processes an array of update operations, where each operation removes specific heap TIDs from a posting list tuple. This is typically used during vacuum operations to remove dead heap TIDs from index tuples.

The function iterates through each update operation, creates a BTVacuumPosting structure containing the original tuple and the list of TIDs to delete, calls _bt_update_posting to generate the updated tuple, and then overwrites the original tuple on the page with the updated version.

Key operations performed:
1. Iterates through the array of update operations
2. For each operation, retrieves the original posting list tuple
3. Creates a BTVacuumPosting structure with TIDs to be removed
4. Calls _bt_update_posting to generate the updated tuple
5. Overwrites the original tuple with the updated version on the page
6. Advances to the next update operation in the array

## Parameters / Member Variables
- `page`: The B-tree page containing the tuples to be updated
- `*updatedoffsets`: Array of offset numbers identifying which tuples on the page need updating
- `*updates`: Array of xl_btree_update structures containing the TIDs to remove from each tuple
- `nupdated`: Number of tuples being updated (length of the arrays)
## Dependencies
- Functions called/Symbols referenced:
  - [PageGetItemId](../P/PageGetItemId.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [_bt_update_posting](_bt_update_posting.md)
  - IndexTupleSize
  - [PageIndexTupleOverwrite](../P/PageIndexTupleOverwrite.md)
  - [palloc](../p/palloc.md)
  - [pfree](../p/pfree.md)
  - memcpy
- Called from (representative examples):
  - [btree_xlog_vacuum](btree_xlog_vacuum.md)
  - [btree_xlog_delete](btree_xlog_delete.md)

## Notes and Other Information
- This is a static helper function used internally during B-tree WAL recovery
- The function processes updates in sequence, with each xl_btree_update structure followed by its array of deleted TIDs
- Uses PANIC level error if tuple overwrite fails, indicating a critical recovery failure
- Memory management includes cleanup of allocated BTVacuumPosting structures and updated tuples
- Part of PostgreSQL's vacuum and dead tuple removal system for B-tree indexes

## Simplified Source

```c
static void btree_xlog_updates(Page page, OffsetNumber *updatedoffsets,
                               xl_btree_update *updates, int nupdated)
{
    BTVacuumPosting vacposting;
    IndexTuple origtuple;
    ItemId itemid;
    Size itemsz;

    // Process each update operation
    for (int i = 0; i < nupdated; i++) {
        // Get original posting list tuple
        itemid = PageGetItemId(page, updatedoffsets[i]);
        origtuple = (IndexTuple) PageGetItem(page, itemid);

        // Create vacuum posting structure with TIDs to delete
        vacposting = palloc(offsetof(BTVacuumPostingData, deletetids) +
                            updates->ndeletedtids * sizeof(uint16));
        vacposting->updatedoffset = updatedoffsets[i];
        vacposting->itup = origtuple;
        vacposting->ndeletedtids = updates->ndeletedtids;
        memcpy(vacposting->deletetids,
               (char *) updates + SizeOfBtreeUpdate,
               updates->ndeletedtids * sizeof(uint16));

        // Generate updated tuple with TIDs removed
        _bt_update_posting(vacposting);

        // Overwrite original tuple with updated version
        itemsz = MAXALIGN(IndexTupleSize(vacposting->itup));
        if (!PageIndexTupleOverwrite(page, updatedoffsets[i],
                                     (Item) vacposting->itup, itemsz))
            elog(PANIC, "failed to update partially dead item");

        // Clean up allocated memory
        pfree(vacposting->itup);
        pfree(vacposting);

        // Advance to next update record
        updates = (xl_btree_update *)
            ((char *) updates + SizeOfBtreeUpdate +
             updates->ndeletedtids * sizeof(uint16));
    }
}
```