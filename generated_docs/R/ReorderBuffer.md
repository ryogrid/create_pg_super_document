# ReorderBuffer

## Location
src/include/replication/reorderbuffer.h: 441 - 544

## Overview
ReorderBuffer is the central management structure for PostgreSQL's logical replication system, coordinating transaction processing, change buffering, memory management, and callback dispatching for logical decoding operations.

## Definition


## Detailed Description
ReorderBuffer serves as the central coordinator for PostgreSQL's logical replication system, managing the entire lifecycle of transaction processing from WAL record decoding through output plugin delivery. It maintains hash tables and ordered lists to efficiently track active transactions, provides comprehensive callback mechanisms for different transaction states (begin, commit, prepare, stream), implements sophisticated memory management with disk spilling capabilities for large transactions, and maintains detailed statistics about processing performance. The structure coordinates between multiple memory contexts for optimal resource management and supports advanced features like transaction streaming, two-phase commit protocols, and catalog change tracking.

## Parameters / Member Variables
- : Hash table for fast transaction lookup by transaction ID
- : Ordered list of potential toplevel transactions by first LSN
- : Ordered list of transactions with base snapshots by snapshot LSN
- : List of transactions that modified system catalogs
- : Cached transaction ID for single-entry lookup optimization
- : Cached transaction pointer for lookup optimization
- : Callback function for transaction begin events
- : Callback function for applying individual changes
- : Callback function for applying truncate operations
- : Callback function for transaction commit events
- : Callback function for logical replication messages
- : Callback function for prepared transaction begin events
- : Callback function for transaction prepare events
- : Callback function for prepared transaction commit
- : Callback function for prepared transaction rollback
- : Callback function for starting transaction streaming
- : Callback function for stopping transaction streaming
- : Callback function for aborting streamed transactions
- : Callback function for preparing streamed transactions
- : Callback function for committing streamed transactions
- : Callback function for streaming individual changes
- : Callback function for streaming messages
- : Callback function for streaming truncate operations
- : Callback function for progress updates during transaction processing
- : Opaque pointer passed to all callback functions
- : Flag indicating whether table rewrites should be output
- : Primary memory context for the reorder buffer
- : Memory context specifically for change allocations
- : Memory context specifically for transaction allocations
- : Memory context specifically for tuple allocations
- : Current restart point for logical decoding recovery
- : Buffer for disk-to-memory conversions during spilling operations
- : Size of the conversion buffer
- : Current memory usage of the reorder buffer
- : Max-heap for managing transaction sizes during memory limit enforcement
- : Statistics counter for transactions spilled to disk
- : Statistics counter for spill-to-disk operation invocations
- : Statistics counter for total bytes spilled to disk
- : Statistics counter for transactions processed via streaming
- : Statistics counter for streaming operation invocations
- : Statistics counter for total bytes processed via streaming
- : Statistics counter for total transactions processed
- : Statistics counter for total bytes processed

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferTXN](ReorderBufferTXN.md)
  - [ReorderBufferChange](ReorderBufferChange.md)
  - Various callback function types (ReorderBufferBeginCB, etc.)
  - [HTAB](../H/HTAB.md)
  - [dlist_head](../d/dlist_head.md)
  - [dclist_head](../d/dclist_head.md)
  - [pairingheap](../p/pairingheap.md)
  - [MemoryContext](../M/MemoryContext.md)
- Called from (representative examples):
  - [ReorderBufferAllocate](ReorderBufferAllocate.md)
  - [ReorderBufferCommit](ReorderBufferCommit.md)
  - [ReorderBufferProcessTXN](ReorderBufferProcessTXN.md)
  - Logical decoding context management functions

## Notes and Other Information
ReorderBuffer is the architectural cornerstone of PostgreSQL's logical replication system, providing a sophisticated framework for managing transaction lifecycle, memory resources, and output plugin interaction. It implements advanced memory management strategies including transaction spilling to disk when memory limits are exceeded, supports transaction streaming for processing large transactions incrementally, and maintains comprehensive statistics for monitoring replication performance. The callback-based architecture allows flexible integration with different output plugins while maintaining consistent transaction ordering and isolation semantics.