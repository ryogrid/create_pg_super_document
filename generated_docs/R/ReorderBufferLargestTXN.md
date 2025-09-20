# ReorderBufferLargestTXN

## Location
[src/backend/replication/logical/reorderbuffer.c:3683-3722](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L3683-L3722)

## Overview
Identifies and returns the largest transaction (by memory size) from the reorder buffer's transaction heap for eviction to disk when memory pressure is detected.

## Definition

```c
static ReorderBufferTXN *
ReorderBufferLargestTXN(ReorderBuffer *rb)
```
## Detailed Description
ReorderBufferLargestTXN is a static function that extracts the transaction with the largest memory footprint from the reorder buffer's transaction heap (). This function is a key component of PostgreSQL's logical replication memory management system.

The function uses a pairing heap data structure to efficiently maintain transactions ordered by size. It retrieves the root element (which represents the largest transaction due to the max-heap property established by ReorderBufferTXNSizeCompare) and returns the corresponding ReorderBufferTXN structure.

The function includes several assertions to ensure data integrity:
- The largest transaction exists (not NULL)
- The transaction has a positive size
- The transaction's size doesn't exceed the total reorder buffer size

This function is typically called when the reorder buffer needs to free up memory by spilling the largest transaction to disk, making it essential for managing memory pressure in logical replication scenarios.

## Parameters / Member Variables
- : Pointer to the ReorderBuffer structure containing the transaction heap to search

## Dependencies
- Functions called/Symbols referenced:
  - pairingheap_container (macro for extracting container structure from heap node)
  - pairingheap_first (retrieves the first/root element from the pairing heap)
  - [ReorderBufferTXN](ReorderBufferTXN.md) (transaction structure type)
- Called from (representative examples):
  - [ReorderBufferCheckMemoryLimit](ReorderBufferCheckMemoryLimit.md) (for identifying transactions to spill during memory pressure)

## Notes and Other Information
- This is a static function, only accessible within reorderbuffer.c
- Relies on the max-heap property maintained by ReorderBufferTXNSizeCompare comparison function
- The function assumes the transaction heap is properly initialized and non-empty
- Part of the memory pressure management subsystem for logical replication
- The returned transaction is a candidate for serialization to disk to free up memory
- Includes defensive assertions to catch potential data corruption or inconsistent state