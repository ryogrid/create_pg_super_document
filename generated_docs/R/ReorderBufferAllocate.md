# ReorderBufferAllocate

## Location
[src/backend/replication/logical/reorderbuffer.c:321-412](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L321-L412)

## Overview
Allocates and initializes a new ReorderBuffer structure used for logical decoding, setting up necessary memory contexts and data structures for transaction reordering.

## Definition

```c
ReorderBuffer *
ReorderBufferAllocate(void)
```
## Detailed Description
ReorderBufferAllocate creates a new ReorderBuffer instance with proper memory management and initialization. It establishes multiple specialized memory contexts for different types of data (changes, transactions, tuples) to optimize memory usage and reduce fragmentation. The function also initializes hash tables for transaction tracking, priority queues for transaction ordering, and various statistical counters. Additionally, it cleans up any serialized transaction state from previous uses of the same replication slot to prevent data duplication.

## Parameters / Member Variables

## Simplified Source

```c
// Simplified version of ReorderBufferAllocate
ReorderBuffer *ReorderBufferAllocate(void) {
    // Create dedicated memory context for the reorder buffer
    MemoryContext new_ctx = AllocSetContextCreate(CurrentMemoryContext,
                                                  "ReorderBuffer",
                                                  ALLOCSET_DEFAULT_SIZES);

    // Allocate the main buffer structure
    ReorderBuffer *buffer = (ReorderBuffer *) MemoryContextAlloc(new_ctx, sizeof(ReorderBuffer));
    buffer->context = new_ctx;

    // Create specialized memory contexts for different data types
    buffer->change_context = SlabContextCreate(new_ctx, "Change",
                                              SLAB_DEFAULT_BLOCK_SIZE,
                                              sizeof(ReorderBufferChange));
    buffer->txn_context = SlabContextCreate(new_ctx, "TXN",
                                           SLAB_DEFAULT_BLOCK_SIZE,
                                           sizeof(ReorderBufferTXN));
    buffer->tup_context = GenerationContextCreate(new_ctx, "Tuples",
                                                  SLAB_DEFAULT_BLOCK_SIZE,
                                                  SLAB_DEFAULT_BLOCK_SIZE,
                                                  SLAB_DEFAULT_BLOCK_SIZE);

    // Set up transaction tracking hash table
    HASHCTL hash_ctl;
    memset(&hash_ctl, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(TransactionId);
    hash_ctl.entrysize = sizeof(ReorderBufferTXNByIdEnt);
    hash_ctl.hcxt = buffer->context;
    buffer->by_txn = hash_create("ReorderBufferByXid", 1000, &hash_ctl,
                                HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

    // Initialize transaction ordering heap
    buffer->txn_heap = pairingheap_allocate(ReorderBufferTXNSizeCompare, NULL);

    // Initialize statistics counters
    buffer->spillTxns = 0;
    buffer->spillCount = 0;
    buffer->spillBytes = 0;
    buffer->streamTxns = 0;
    buffer->streamCount = 0;
    buffer->streamBytes = 0;
    buffer->totalTxns = 0;
    buffer->totalBytes = 0;

    // Initialize lists and reset state
    dlist_init(&buffer->toplevel_by_lsn);
    dlist_init(&buffer->txns_by_base_snapshot_lsn);
    dclist_init(&buffer->catchange_txns);
    buffer->by_txn_last_xid = InvalidTransactionId;
    buffer->by_txn_last_txn = NULL;
    buffer->current_restart_decoding_lsn = InvalidXLogRecPtr;

    // Clean up any stale serialized data from previous uses
    ReorderBufferCleanupSerializedTXNs(NameStr(MyReplicationSlot->data.name));

    return buffer;
}
```

Key simplifications made:
- Grouped related initialization code together
- Added clear comments for each major section
- Simplified memory context setup while preserving functionality
- Combined related field initializations
- Focused on core allocation and initialization logic

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)  
  - [SlabContextCreate](../S/SlabContextCreate.md)
  - [GenerationContextCreate](../G/GenerationContextCreate.md)
  - [hash_create](../h/hash_create.md)
  - [pairingheap_allocate](../p/pairingheap_allocate.md)
  - [dlist_init](../d/dlist_init.md)
  - [dclist_init](../d/dclist_init.md)
  - [ReorderBufferCleanupSerializedTXNs](ReorderBufferCleanupSerializedTXNs.md)
- Called from (representative examples):
  - [StartupDecodingContext](../S/StartupDecodingContext.md)

## Notes and Other Information
- Creates specialized memory contexts: main context, change_context (slab), txn_context (slab), and tup_context (generation)
- The tuple context uses a fixed-size memory block to minimize fragmentation from long-running transactions
- Initializes a hash table for transaction lookup by transaction ID with 1000 initial buckets
- Sets up a pairing heap for ordering transactions by size
- Initializes various statistics counters for spill/stream operations
- Always cleans up serialized state from previous slot uses to prevent duplicated transactions
- Requires MyReplicationSlot to be set (asserted at function start)