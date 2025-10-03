# rewrite_heap_tuple

## Location
[src/backend/access/heap/rewriteheap.c:341-542](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/rewriteheap.c#L341-L542)

## Overview
Rewrites and inserts a tuple into the new heap during a table rewrite operation, handling tuple visibility information, update chains, and cross-references between old and new tuple locations.

## Definition

```c
void
rewrite_heap_tuple(RewriteState state,
				   HeapTuple old_tuple, HeapTuple new_tuple)
```
## Detailed Description
The `rewrite_heap_tuple` function is responsible for processing individual tuples during a heap rewrite operation. It copies visibility information from the original tuple to the new tuple, applies freezing to old transactions, and manages complex update chain relationships. The function handles the intricate task of maintaining tuple update chains across the rewrite by using hash tables to track unresolved tuple references and old-to-new TID mappings.

The function manages update chains by checking if a tuple is part of an update chain and either resolves existing forward references or creates new mapping entries for future resolution. It uses a loop to process cascading tuple chain resolutions, where resolving one tuple may trigger the resolution of previously unresolved tuples that were waiting for this tuple's location.

## Parameters / Member Variables
- `state`: The RewriteState structure containing rewrite context, hash tables, and configuration parameters
- `old_tuple`: The original HeapTuple from the old heap relation that serves as the source
- `new_tuple`: The new HeapTuple to be written to the new heap relation (modified by this function)

## Dependencies
- Functions called/Symbols referenced:
  - [heap_freeze_tuple](../h/heap_freeze_tuple.md)
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md)
  - [HeapTupleHeaderIsOnlyLocked](../H/HeapTupleHeaderIsOnlyLocked.md)
  - HeapTupleHeaderIndicatesMovedPartitions
  - [ItemPointerEquals](../I/ItemPointerEquals.md)
  - HeapTupleHeaderGetUpdateXid
  - [hash_search](../h/hash_search.md)
  - [heap_copytuple](../h/heap_copytuple.md)
  - [raw_heap_insert](raw_heap_insert.md)
  - [logical_rewrite_heap_tuple](../l/logical_rewrite_heap_tuple.md)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - HeapTupleHeaderGetXmin
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [reform_and_rewrite_tuple](reform_and_rewrite_tuple.md)

## Notes and Other Information
- Copies transaction visibility information from old to new tuple while clearing HOT status bits
- Applies tuple freezing to prevent transaction wraparound issues in the new heap
- Manages complex update chain resolution using two hash tables: rs_unresolved_tups and rs_old_new_tid_map
- Handles cascading chain resolution through a loop that processes newly resolved tuples
- Integrates with logical replication by calling logical_rewrite_heap_tuple for change tracking
- Uses temporary memory context switching to ensure proper memory management
- May defer tuple insertion if it's part of an unresolved update chain
- Properly handles tuple freeing to prevent memory leaks in complex chain scenarios

## Simplified Source

```c
void
rewrite_heap_tuple(RewriteState state, HeapTuple old_tuple, HeapTuple new_tuple)
{
    MemoryContext old_cxt = MemoryContextSwitchTo(state->rs_cxt);
    ItemPointerData old_tid;
    TidHashKey hashkey;
    bool found, free_new = false;

    // Copy visibility information and freeze old transactions
    memcpy(&new_tuple->t_data->t_choice.t_heap,
           &old_tuple->t_data->t_choice.t_heap,
           sizeof(HeapTupleFields));

    new_tuple->t_data->t_infomask &= ~HEAP_XACT_MASK;
    new_tuple->t_data->t_infomask2 &= ~HEAP2_XACT_MASK;
    new_tuple->t_data->t_infomask |= old_tuple->t_data->t_infomask & HEAP_XACT_MASK;

    // Apply tuple freezing to prevent wraparound
    heap_freeze_tuple(new_tuple->t_data,
                      state->rs_old_rel->rd_rel->relfrozenxid,
                      state->rs_old_rel->rd_rel->relminmxid,
                      state->rs_freeze_xid,
                      state->rs_cutoff_multi);

    ItemPointerSetInvalid(&new_tuple->t_data->t_ctid);

    // Check if this tuple is part of an update chain
    if (!((old_tuple->t_data->t_infomask & HEAP_XMAX_INVALID) ||
          HeapTupleHeaderIsOnlyLocked(old_tuple->t_data)) &&
        !HeapTupleHeaderIndicatesMovedPartitions(old_tuple->t_data) &&
        !ItemPointerEquals(&(old_tuple->t_self), &(old_tuple->t_data->t_ctid))) {

        // Look for existing mapping to resolve forward reference
        memset(&hashkey, 0, sizeof(hashkey));
        hashkey.xmin = HeapTupleHeaderGetUpdateXid(old_tuple->t_data);
        hashkey.tid = old_tuple->t_data->t_ctid;

        OldToNewMapping mapping = (OldToNewMapping)
            hash_search(state->rs_old_new_tid_map, &hashkey, HASH_FIND, NULL);

        if (mapping != NULL) {
            // Forward reference resolved - set ctid and proceed
            new_tuple->t_data->t_ctid = mapping->new_tid;
            hash_search(state->rs_old_new_tid_map, &hashkey, HASH_REMOVE, &found);
        }
        else {
            // Store unresolved tuple for later processing
            UnresolvedTup unresolved = hash_search(state->rs_unresolved_tups, &hashkey,
                                                   HASH_ENTER, &found);
            unresolved->old_tid = old_tuple->t_self;
            unresolved->tuple = heap_copytuple(new_tuple);
            MemoryContextSwitchTo(old_cxt);
            return;
        }
    }

    // Write tuple and handle chain resolution
    old_tid = old_tuple->t_self;

    for (;;) {
        ItemPointerData new_tid;

        // Insert tuple and get its new location
        raw_heap_insert(state, new_tuple);
        new_tid = new_tuple->t_self;

        logical_rewrite_heap_tuple(state, old_tid, new_tuple);

        // Check if this resolves a waiting tuple (B in update pair)
        if ((new_tuple->t_data->t_infomask & HEAP_UPDATED) &&
            !TransactionIdPrecedes(HeapTupleHeaderGetXmin(new_tuple->t_data),
                                   state->rs_oldest_xmin)) {

            memset(&hashkey, 0, sizeof(hashkey));
            hashkey.xmin = HeapTupleHeaderGetXmin(new_tuple->t_data);
            hashkey.tid = old_tid;

            UnresolvedTup unresolved = hash_search(state->rs_unresolved_tups, &hashkey,
                                                   HASH_FIND, NULL);

            if (unresolved != NULL) {
                // Process waiting tuple from chain
                if (free_new)
                    heap_freetuple(new_tuple);
                new_tuple = unresolved->tuple;
                free_new = true;
                old_tid = unresolved->old_tid;
                new_tuple->t_data->t_ctid = new_tid;

                hash_search(state->rs_unresolved_tups, &hashkey, HASH_REMOVE, &found);
                continue;  // Loop to insert the waiting tuple
            }
            else {
                // Create mapping for future resolution
                OldToNewMapping mapping = hash_search(state->rs_old_new_tid_map, &hashkey,
                                                      HASH_ENTER, &found);
                mapping->new_tid = new_tid;
            }
        }

        if (free_new)
            heap_freetuple(new_tuple);
        break;
    }

    MemoryContextSwitchTo(old_cxt);
}
```