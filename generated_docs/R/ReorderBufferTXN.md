# ReorderBufferTXN

## Location
[src/include/replication/reorderbuffer.h:259-438](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/reorderbuffer.h#L259-L438)

## Overview
ReorderBufferTXN represents a complete transaction in PostgreSQL's logical replication system, containing all changes, metadata, and state information necessary to reconstruct and replay the transaction.

## Definition


## Detailed Description
ReorderBufferTXN is the central data structure representing a transaction within PostgreSQL's logical replication reorder buffer system. It maintains comprehensive transaction state including all changes, snapshots, command IDs, timing information, and hierarchical relationships between parent and child transactions. The structure supports both regular transactions and prepared transactions (two-phase commit), handles toast data reconstruction, manages catalog tuple visibility mappings, and provides memory management for spilling large transactions to disk when memory limits are exceeded.

## Parameters / Member Variables
- : Bitfield containing transaction state flags and properties
- : Transaction ID of this transaction (can be toplevel or subtransaction)
- : Transaction ID of the top-level transaction if this is a subtransaction
- : Global transaction identifier for prepared transactions (two-phase commit)
- : LSN of the first WAL record carrying data for this transaction
- : LSN of the commit/prepare/abort record or latest spilled change
- : LSN pointing to the end of the commit record plus one
- : Pointer to the top-level transaction (NULL for top-level transactions)
- : LSN from which decoding can be restarted to recover this transaction
- : Replication origin identifier for change tracking
- : LSN at the origin where this change was generated
- : Timestamp when transaction was committed
- : Timestamp when transaction was prepared
- : Timestamp when transaction was aborted
- : Snapshot used for decoding changes until catalog modifications occur
- : LSN where the base snapshot was taken
- : List node for linking transactions by base snapshot LSN
- : Current snapshot for streaming transactions
- : Current command ID for streaming transactions
- : Total number of changes in this transaction (excluding subtransactions)
- : Number of changes currently stored in memory
- : Doubly-linked list of ReorderBufferChange structures
- : List of catalog tuple command ID mappings
- : Number of tuple CID mappings
- : Hash table for efficient tuple CID lookup
- : Hash table for assembling TOAST chunks
- : List of non-aborted subtransactions
- : Number of subtransactions
- : Number of cache invalidation messages
- : Array of cache invalidation messages
- : List node for transaction organization
- : List node for catalog-modifying transactions
- : Pairing heap node for transaction priority management
- : Memory size of this transaction's changes
- : Total memory size including subtransactions
- : Flag indicating concurrent abort detection
- : Private data pointer for output plugins
- : Number of distributed invalidation messages
- : Array of distributed invalidation messages

## Dependencies
- Functions called/Symbols referenced:
  - bits32
  - [ReorderBufferTXN](ReorderBufferTXN.md) (self-reference for hierarchy)
  - RepOriginId
  - [dlist_node](../d/dlist_node.md)
  - CommandId
  - [dlist_head](../d/dlist_head.md)
  - [HTAB](../H/HTAB.md)
  - [pairingheap_node](../p/pairingheap_node.md)
  - SharedInvalidationMessage
- Called from (representative examples):
  - [ReorderBufferGetTXN](ReorderBufferGetTXN.md)
  - [ReorderBufferCommit](ReorderBufferCommit.md)
  - [ReorderBufferProcessTXN](ReorderBufferProcessTXN.md)
  - pgoutput transaction callback functions

## Notes and Other Information
This structure is fundamental to PostgreSQL's logical replication architecture and supports advanced features like hierarchical transactions, memory management with disk spilling, TOAST data reconstruction, and two-phase commit protocols. The transaction maintains detailed timing and LSN information for proper ordering and restart capabilities. The structure efficiently handles both small and large transactions through its memory management system and supports streaming of large transactions to avoid memory exhaustion.