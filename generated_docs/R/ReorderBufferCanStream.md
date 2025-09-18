# ReorderBufferCanStream

## Location
src/backend/replication/logical/reorderbuffer.c: 4150 - 4158

## Overview
Determines whether the current logical decoding output plugin supports streaming of in-progress transactions.

## Definition
```c
static inline bool ReorderBufferCanStream(ReorderBuffer *rb)
```

## Detailed Description
This inline function provides a simple check to determine if the logical decoding context associated with the reorder buffer supports streaming functionality. Streaming allows logical replication to send changes from large transactions before they commit, rather than waiting for the entire transaction to complete. This is particularly useful for handling large transactions that might otherwise cause memory issues or long delays.

The function accesses the LogicalDecodingContext through the reorder buffer's private_data field and returns the streaming flag value. This enables other parts of the reorder buffer system to conditionally enable streaming-related functionality.

## Parameters / Member Variables
- `rb`: ReorderBuffer instance containing the logical decoding context

## Dependencies
- Functions called/Symbols referenced:
  - LogicalDecodingContext (accessed via rb->private_data)
- Called from (representative examples):
  - ReorderBufferCanStartStreaming (streaming capability check)
  - ReorderBufferProcessPartialChange (partial change processing)
  - IsInsertOrUpdate (change type determination)

## Notes and Other Information
- Declared as static inline for performance optimization
- Simple accessor function with minimal computational overhead
- Streaming support is determined by the output plugin configuration
- Used as a prerequisite check before attempting streaming operations
- The streaming capability is typically set during logical decoding context initialization