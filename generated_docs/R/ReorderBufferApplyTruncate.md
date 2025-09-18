# ReorderBufferApplyTruncate

## Location
[src/backend/replication/logical/reorderbuffer.c:2026-2039](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L2026-L2039)

## Overview
ReorderBufferApplyTruncate is a helper function that applies TRUNCATE operations during transaction processing, choosing between streaming and regular apply modes based on the replication context.

## Definition
```c
static inline void ReorderBufferApplyTruncate(ReorderBuffer *rb, ReorderBufferTXN *txn, int nrelations, Relation *relations, ReorderBufferChange *change, bool streaming)
```

## Detailed Description
This function serves as a dispatcher for applying TRUNCATE operations in logical replication. Similar to ReorderBufferApplyChange, it provides a unified interface that abstracts the choice between two different application modes for TRUNCATE operations:

1. **Streaming mode**: When `streaming` is true, it calls `rb->stream_truncate()` to apply the truncate operation as part of a streaming transaction
2. **Regular mode**: When `streaming` is false, it calls `rb->apply_truncate()` to apply the truncate operation in the standard non-streaming manner

TRUNCATE operations are special in logical replication because they can affect multiple relations simultaneously and require different handling compared to regular INSERT/UPDATE/DELETE operations.

## Parameters / Member Variables
- `rb`: ReorderBuffer pointer - the reorder buffer context containing callback functions
- `txn`: ReorderBufferTXN pointer - the transaction containing this truncate operation
- `nrelations`: int - the number of relations being truncated
- `relations`: Relation* - array of relations being truncated by this operation
- `change`: ReorderBufferChange pointer - the specific truncate change to be applied
- `streaming`: bool - flag indicating whether to use streaming mode (true) or regular mode (false)

## Dependencies
- Functions called/Symbols referenced:
  - rb->stream_truncate (callback for streaming mode)
  - rb->apply_truncate (callback for regular mode)
- Called from (representative examples):
  - [ReorderBufferProcessTXN](ReorderBufferProcessTXN.md)

## Notes and Other Information
- This is a static inline function for performance optimization in the transaction processing path
- The function provides abstraction over the streaming vs non-streaming truncate application logic
- TRUNCATE operations can affect multiple relations simultaneously, hence the nrelations and relations parameters
- The actual truncate application is delegated to callback functions in the ReorderBuffer structure
- Part of PostgreSQL's logical replication system for processing DDL operations during transaction replay
- The streaming parameter determines the execution path, enabling support for both traditional and streaming logical replication modes
- Complements ReorderBufferApplyChange by handling the specific case of TRUNCATE operations
- Helper function design pattern reduces code duplication in ReorderBufferProcessTXN