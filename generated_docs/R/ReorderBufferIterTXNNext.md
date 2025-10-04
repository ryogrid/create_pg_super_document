# ReorderBufferIterTXNNext

## Location
[src/backend/replication/logical/reorderbuffer.c:1408-1499](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L1408-L1499)

## Overview
Returns the next change in LSN order when iterating over a transaction and its subtransactions, managing both in-memory and disk-based changes.

## Definition
```c
static ReorderBufferChange *ReorderBufferIterTXNNext(ReorderBuffer *rb, ReorderBufferIterTXNState *state)
```

## Detailed Description
This function implements the core iteration logic for the k-way merge algorithm used to traverse changes from a transaction hierarchy in LSN order. It manages the binary heap to efficiently determine which change has the lowest LSN among all active transactions and subtransactions.

The function handles several scenarios:
1. **Memory cleanup**: Frees changes from previous iterations that were deferred for cleanup
2. **In-memory changes**: When more changes exist in memory, updates the heap with the next change
3. **Disk-based changes**: When memory changes are exhausted, attempts to restore more changes from disk storage
4. **Transaction completion**: Removes transactions from the heap when no more changes exist

The function includes sophisticated memory management, deferring cleanup of change records until the next iteration to avoid issues with record reuse during disk restoration. It also tracks total bytes processed and provides debug logging for disk restoration operations.

## Parameters / Member Variables
- `rb`: The reorder buffer instance managing the transactions
- `state`: The iterator state containing the binary heap and transaction entries

## Dependencies
- Functions called/Symbols referenced:
  - [binaryheap_first](../b/binaryheap_first.md), binaryheap_replace_first, binaryheap_remove_first (binary heap operations)
  - [dlist_is_empty](../d/dlist_is_empty.md), dlist_has_next, dlist_next_node, dlist_container, dlist_delete, dlist_push_tail, dlist_pop_head_node, dlist_head_element (doubly-linked list operations)
  - [ReorderBufferReturnChange](ReorderBufferReturnChange.md) (returns change to memory pool)
  - [ReorderBufferRestoreChanges](ReorderBufferRestoreChanges.md) (restores changes from disk)
  - [DatumGetInt32](../D/DatumGetInt32.md) (extracts integer from Datum)
  - elog (logging function)
- Called from (representative examples):
  - [ReorderBufferProcessTXN](ReorderBufferProcessTXN.md) (during transaction processing)
  - IsInsertOrUpdate (during change processing)

## Notes and Other Information
- This is a static function used internally for iterator traversal
- Returns NULL when no further changes exist in any transaction
- Implements deferred memory cleanup to handle change record reuse during disk restoration
- Updates total bytes processed counter when restoring changes from disk
- Provides DEBUG2 logging for disk restoration operations
- Maintains heap invariants by updating or removing entries as transactions are processed
- Handles both the common case of in-memory changes and the complex case of disk-based serialized changes
- Essential component of PostgreSQLs logical replication change ordering mechanism
- Memory management ensures proper cleanup while avoiding premature deallocation during record reuse

## Simplified Source

```c
static ReorderBufferChange *
ReorderBufferIterTXNNext(ReorderBuffer *rb, ReorderBufferIterTXNState *state)
{
    ReorderBufferChange *change;
    ReorderBufferIterTXNEntry *entry;
    int32 off;

    // No more changes available
    if (state->heap->bh_size == 0)
        return NULL;

    // Get the transaction entry with the lowest LSN
    off = DatumGetInt32(binaryheap_first(state->heap));
    entry = &state->entries[off];

    // Clean up change from previous iteration
    if (!dlist_is_empty(&state->old_change))
    {
        change = dlist_container(ReorderBufferChange, node,
                                 dlist_pop_head_node(&state->old_change));
        ReorderBufferReturnChange(rb, change, true);
    }

    change = entry->change;

    // Check if there are more in-memory changes in this transaction
    if (dlist_has_next(&entry->txn->changes, &entry->change->node))
    {
        dlist_node *next = dlist_next_node(&entry->txn->changes, &change->node);
        ReorderBufferChange *next_change = dlist_container(ReorderBufferChange, node, next);

        // Update heap entry with next change from same transaction
        state->entries[off].lsn = next_change->lsn;
        state->entries[off].change = next_change;
        binaryheap_replace_first(state->heap, Int32GetDatum(off));

        return change;
    }

    // Try to load more changes from disk if available
    if (entry->txn->nentries != entry->txn->nentries_mem)
    {
        // Move current change to cleanup list to avoid reuse issues
        dlist_delete(&change->node);
        dlist_push_tail(&state->old_change, &change->node);

        // Update total bytes processed
        rb->totalBytes += entry->txn->size;

        // Attempt to restore changes from disk
        if (ReorderBufferRestoreChanges(rb, entry->txn, &entry->file,
                                        &state->entries[off].segno))
        {
            // Successfully restored - update heap with first restored change
            ReorderBufferChange *next_change =
                dlist_head_element(ReorderBufferChange, node, &entry->txn->changes);

            state->entries[off].lsn = next_change->lsn;
            state->entries[off].change = next_change;
            binaryheap_replace_first(state->heap, Int32GetDatum(off));

            return change;
        }
    }

    // No more changes for this transaction - remove from heap
    binaryheap_remove_first(state->heap);
    return change;
}
```