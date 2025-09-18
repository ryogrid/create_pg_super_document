# ReorderBufferAllocate

## Location
src/backend/replication/logical/reorderbuffer.c: 321 - 412

## Overview
Allocates and initializes a new ReorderBuffer structure used for logical decoding, setting up necessary memory contexts and data structures for transaction reordering.

## Definition


## Detailed Description
ReorderBufferAllocate creates a new ReorderBuffer instance with proper memory management and initialization. It establishes multiple specialized memory contexts for different types of data (changes, transactions, tuples) to optimize memory usage and reduce fragmentation. The function also initializes hash tables for transaction tracking, priority queues for transaction ordering, and various statistical counters. Additionally, it cleans up any serialized transaction state from previous uses of the same replication slot to prevent data duplication.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)  
  - [SlabContextCreate](../S/SlabContextCreate.md)
  - [GenerationContextCreate](../G/GenerationContextCreate.md)
  - [hash_create](../h/hash_create.md)
  - pairingheap_allocate
  - [dlist_init](../d/dlist_init.md)
  - [dclist_init](../d/dclist_init.md)
  - [ReorderBufferCleanupSerializedTXNs](ReorderBufferCleanupSerializedTXNs.md)
- Called from (representative examples):
  - StartupDecodingContext

## Notes and Other Information
- Creates specialized memory contexts: main context, change_context (slab), txn_context (slab), and tup_context (generation)
- The tuple context uses a fixed-size memory block to minimize fragmentation from long-running transactions
- Initializes a hash table for transaction lookup by transaction ID with 1000 initial buckets
- Sets up a pairing heap for ordering transactions by size
- Initializes various statistics counters for spill/stream operations
- Always cleans up serialized state from previous slot uses to prevent duplicated transactions
- Requires MyReplicationSlot to be set (asserted at function start)