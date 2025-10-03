# heapam_tuple_update

## Location
[src/backend/access/heap/heapam_handler.c:315-359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L315-L359)

## Overview
Implements the heap table access method interface for updating tuples, handling the complexities of HOT (Heap-Only Tuple) updates and determining appropriate index update strategies.

## Definition
```c
static TM_Result heapam_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot, CommandId cid, Snapshot snapshot, Snapshot crosscheck, bool wait, TM_FailureData *tmfd, LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
```

## Detailed Description
This function provides the heap-specific implementation of tuple updates within PostgreSQL's table access method framework. It handles the sophisticated logic around tuple updates, including HOT (Heap-Only Tuple) optimizations and index maintenance decisions.

The function first retrieves the heap tuple from the slot and sets the table OID appropriately. It then delegates to `heap_update` for the core update operation. After the update, it determines the appropriate index update strategy based on whether the update resulted in a HOT update (where the new tuple shares the same index entries as the old one) or a regular update requiring full index maintenance.

The index update decision logic distinguishes between:
- TU_All: Update all indexes (non-HOT updates)
- TU_Summarizing: Update only summarizing indexes (HOT updates affecting summarized columns)
- TU_None: No index updates needed (pure HOT updates or failed updates)

## Parameters / Member Variables
- `relation`: The heap relation containing the tuple to update
- `otid`: ItemPointer to the old tuple being updated
- `slot`: TupleTableSlot containing the new tuple data
- `cid`: CommandId for visibility and concurrency control
- `snapshot`: Snapshot for visibility checks during the update
- `crosscheck`: Additional snapshot for cross-transaction validation
- `wait`: Boolean indicating whether to wait if the tuple is locked
- `tmfd`: TM_FailureData structure to receive failure details if update fails
- `lockmode`: Pointer to LockTupleMode, may be modified during operation
- `update_indexes`: Pointer to TU_UpdateIndexes indicating which indexes need updates

## Dependencies
- Functions called/Symbols referenced:
  - [ExecFetchSlotHeapTuple](../E/ExecFetchSlotHeapTuple.md)
  - [heap_update](heap_update.md)
  - [ItemPointerCopy](../I/ItemPointerCopy.md)
  - HeapTupleIsHeapOnly
  - RelationGetRelid (implicitly through slot->tts_tableOid assignment)
- Types referenced:
  - TM_Result, TM_FailureData, CommandId, LockTupleMode, TU_UpdateIndexes
  - TM_Ok, TU_All, TU_Summarizing, TU_None (enumeration values)
- Called from (representative examples):
  - Used through table access method interface (no direct callers found in indexed code)

## Notes and Other Information
- This is a static function within heapam_handler.c, part of the heap table access method implementation
- Handles memory management by freeing the tuple if `ExecFetchSlotHeapTuple` allocated it
- The function updates `slot->tts_tid` with the location of the new tuple after update
- Contains sophisticated logic for determining index update requirements based on HOT update semantics
- HOT updates are a PostgreSQL optimization that avoids updating indexes when only non-indexed columns change
- The function includes assertions to validate the consistency between update results and index update flags
- Part of PostgreSQL's pluggable table access method architecture

## Simplified Source

```c
static TM_Result
heapam_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
                    CommandId cid, Snapshot snapshot, Snapshot crosscheck,
                    bool wait, TM_FailureData *tmfd,
                    LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
{
    // Extract heap tuple from slot
    bool shouldFree = true;
    HeapTuple tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);
    TM_Result result;

    // Set table OID in both slot and tuple
    slot->tts_tableOid = RelationGetRelid(relation);
    tuple->t_tableOid = slot->tts_tableOid;

    // Perform the actual update
    result = heap_update(relation, otid, tuple, cid, crosscheck, wait,
                         tmfd, lockmode, update_indexes);

    // Copy new tuple location back to slot
    ItemPointerCopy(&tuple->t_self, &slot->tts_tid);

    // Determine index update strategy based on result
    if (result != TM_Ok) {
        *update_indexes = TU_None;  // Failed update
    } else if (!HeapTupleIsHeapOnly(tuple)) {
        // Non-HOT update: all indexes need updating
        Assert(*update_indexes == TU_All);
    } else {
        // HOT update: only summarizing indexes or none
        Assert((*update_indexes == TU_Summarizing) ||
               (*update_indexes == TU_None));
    }

    // Clean up allocated memory if needed
    if (shouldFree)
        pfree(tuple);

    return result;
}
```