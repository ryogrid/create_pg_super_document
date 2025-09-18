# ReorderBufferApplyChange

## Location
src/backend/replication/logical/reorderbuffer.c: 2012 - 2025

## Overview
ReorderBufferApplyChange is a helper function that applies individual changes during transaction processing, choosing between streaming and regular apply modes based on the context.

## Definition
```c
static inline void ReorderBufferApplyChange(ReorderBuffer *rb, ReorderBufferTXN *txn, Relation relation, ReorderBufferChange *change, bool streaming)
```

## Detailed Description
This function serves as a dispatcher for applying individual changes in logical replication. It provides a unified interface that abstracts the choice between two different application modes:

1. **Streaming mode**: When `streaming` is true, it calls `rb->stream_change()` to apply the change as part of a streaming transaction
2. **Regular mode**: When `streaming` is false, it calls `rb->apply_change()` to apply the change in the standard non-streaming manner

The function encapsulates the decision logic for change application, making the calling code cleaner and more maintainable by removing the conditional logic from the main processing flow.

## Parameters / Member Variables
- `rb`: ReorderBuffer pointer - the reorder buffer context containing callback functions
- `txn`: ReorderBufferTXN pointer - the transaction containing this change
- `relation`: Relation pointer - the database relation affected by this change
- `change`: ReorderBufferChange pointer - the specific change to be applied
- `streaming`: bool - flag indicating whether to use streaming mode (true) or regular mode (false)

## Dependencies
- Functions called/Symbols referenced:
  - rb->stream_change (callback for streaming mode)
  - rb->apply_change (callback for regular mode)
- Called from (representative examples):
  - ReorderBufferProcessTXN

## Notes and Other Information
- This is a static inline function for performance optimization in the transaction processing path
- The function provides abstraction over the streaming vs non-streaming change application logic
- The actual change application is delegated to callback functions in the ReorderBuffer structure
- Part of PostgreSQL's logical replication system for processing transaction changes
- The streaming parameter determines the execution path, enabling support for both traditional and streaming logical replication modes
- Helper function design pattern reduces code duplication in ReorderBufferProcessTXN