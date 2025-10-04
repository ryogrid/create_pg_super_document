# ReorderBufferChangeMemoryUpdate

## Location
[src/backend/replication/logical/reorderbuffer.c:3260-3330](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L3260-L3330)

## Overview
ReorderBufferChangeMemoryUpdate maintains memory usage counters for the reorder buffer and individual transactions, enabling memory limit enforcement and efficient transaction eviction during logical decoding.

## Definition
```c
static void ReorderBufferChangeMemoryUpdate(ReorderBuffer *rb, ReorderBufferChange *change, ReorderBufferTXN *txn, bool addition, Size sz)
```

## Detailed Description
This internal function manages memory accounting for the reorder buffer system by maintaining dual counters: one for the overall reorder buffer memory usage and another for individual transaction memory usage. These counters are essential for memory management during logical decoding, allowing the system to decide when memory limits are reached and which transactions should be evicted.

The function implements special handling for streaming scenarios, where subtransaction counters are not maintained since only top-level transactions can be streamed individually. It also manages a max-heap data structure that allows efficient selection of the largest transaction for eviction when memory pressure occurs.

The function excludes tuple CID changes from memory accounting since these internal changes are not evicted when memory limits are reached, preventing unnecessary spill attempts.

## Parameters
- `rb`: Pointer to the ReorderBuffer instance tracking overall memory usage
- `change`: Pointer to the ReorderBufferChange being accounted for (can be NULL if txn is provided)
- `txn`: Pointer to the ReorderBufferTXN to update (can be NULL if change is provided)
- `addition`: Boolean indicating whether this is an addition (true) or removal (false) of memory
- `sz`: Size in bytes to add or subtract from the counters

## Dependencies
- Functions called/Symbols referenced:
  - rbtxn_get_toptxn
  - [pairingheap_remove](../p/pairingheap_remove.md)
  - [pairingheap_add](../p/pairingheap_add.md)
  - REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID
- Called from (representative examples):
  - IsInsertOrUpdate
  - [ReorderBufferReturnChange](ReorderBufferReturnChange.md)
  - [ReorderBufferQueueChange](ReorderBufferQueueChange.md)
  - [ReorderBufferCleanupTXN](ReorderBufferCleanupTXN.md)
  - [ReorderBufferTruncateTXN](ReorderBufferTruncateTXN.md)
  - [ReorderBufferSerializeTXN](ReorderBufferSerializeTXN.md)
  - [ReorderBufferRestoreChange](ReorderBufferRestoreChange.md)
  - [ReorderBufferToastReplace](ReorderBufferToastReplace.md)

## Notes and Other Information
- Either txn or change must be non-NULL; if txn is NULL, the function uses change->txn
- Tuple CID changes are ignored since they are not subject to eviction
- The function maintains both transaction-level and top-level transaction counters for streaming support
- Uses a pairing heap to efficiently track the largest transactions for eviction decisions
- Heap operations are performed to maintain the correct ordering after size updates
- Includes assertions to verify memory accounting consistency
- The function is static and intended for internal use within the reorder buffer implementation

## Simplified Source

```c
static void ReorderBufferChangeMemoryUpdate(ReorderBuffer *rb,
                                           ReorderBufferChange *change,
                                           ReorderBufferTXN *txn,
                                           bool addition, Size sz)
{
    ReorderBufferTXN *toptxn;

    // Skip tuple CID changes as they're not evicted
    if (change && change->action == REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID)
        return;

    // No-op for zero size changes
    if (sz == 0)
        return;

    // Use transaction from change if txn is NULL
    if (txn == NULL)
        txn = change->txn;

    // Get top-level transaction for streaming support
    toptxn = rbtxn_get_toptxn(txn);

    if (addition)
    {
        // Add memory to counters
        Size oldsize = txn->size;
        txn->size += sz;
        rb->size += sz;
        toptxn->total_size += sz;

        // Update max-heap for efficient eviction selection
        if (oldsize != 0)
            pairingheap_remove(rb->txn_heap, &txn->txn_node);
        pairingheap_add(rb->txn_heap, &txn->txn_node);
    }
    else
    {
        // Remove memory from counters
        txn->size -= sz;
        rb->size -= sz;
        toptxn->total_size -= sz;

        // Update max-heap
        pairingheap_remove(rb->txn_heap, &txn->txn_node);
        if (txn->size != 0)
            pairingheap_add(rb->txn_heap, &txn->txn_node);
    }
}
```