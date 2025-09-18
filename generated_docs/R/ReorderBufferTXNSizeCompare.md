# ReorderBufferTXNSizeCompare

## Location
[src/backend/replication/logical/reorderbuffer.c:3667-3682](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L3667-L3682)

## Overview
A comparison function that orders ReorderBufferTXN transactions by their memory size, used for maintaining a priority queue of transactions in the reorder buffer.

## Definition


## Detailed Description
ReorderBufferTXNSizeCompare is a static comparison function designed to work with PostgreSQL's pairing heap data structure. It compares two ReorderBufferTXN transactions based on their memory consumption ( field) to establish ordering within a priority queue or heap.

The function extracts ReorderBufferTXN structures from pairing heap nodes using the  macro, then performs a three-way comparison of their sizes:
- Returns -1 if the first transaction is smaller
- Returns 1 if the first transaction is larger  
- Returns 0 if both transactions have equal size

This comparison function is essential for memory management in logical replication, allowing the system to identify and prioritize transactions based on their memory footprint for operations like spilling to disk or cleanup.

## Parameters / Member Variables
- : Pointer to the first pairing heap node containing a ReorderBufferTXN
- : Pointer to the second pairing heap node containing a ReorderBufferTXN  
- : Unused argument parameter (required by pairing heap interface)

## Dependencies
- Functions called/Symbols referenced:
  - pairingheap_const_container (macro for extracting container structure from heap node)
  - [ReorderBufferTXN](ReorderBufferTXN.md) (transaction structure type)
  - [pairingheap_node](../p/pairingheap_node.md) (heap node structure type)
- Called from (representative examples):
  - [ReorderBufferAllocate](ReorderBufferAllocate.md) (likely during heap initialization)
  - Used indirectly through pairing heap operations for transaction ordering

## Notes and Other Information
- This is a static function, only accessible within reorderbuffer.c
- Follows the standard comparison function interface required by pairing heap implementations
- The function assumes both heap nodes contain valid ReorderBufferTXN structures
- Critical for memory pressure management in logical replication by enabling size-based transaction prioritization
- The  parameter is unused but required to match the pairing heap comparison function signature