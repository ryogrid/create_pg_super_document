# ReorderBufferFree

## Location
[src/backend/replication/logical/reorderbuffer.c:413-430](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L413-L430)

## Overview
Deallocates a ReorderBuffer instance by deleting its memory context and cleaning up any associated serialized transaction data on disk.

## Definition

```c
void
ReorderBufferFree(ReorderBuffer *rb)
```
## Detailed Description
ReorderBufferFree performs complete cleanup of a ReorderBuffer instance. It deallocates all memory associated with the buffer by deleting its entire memory context, which automatically frees all child contexts (change_context, txn_context, tup_context) and associated data structures. Additionally, it cleans up any unconsumed serialized transaction data that may have been written to disk during the buffer's lifetime.

## Parameters / Member Variables
- `rb`: Pointer to the ReorderBuffer structure to be freed

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - [ReorderBufferCleanupSerializedTXNs](ReorderBufferCleanupSerializedTXNs.md)
- Called from (representative examples):
  - [FreeDecodingContext](../F/FreeDecodingContext.md)

## Notes and Other Information
- Uses the memory context hierarchy for efficient cleanup - deleting the main context automatically frees all child contexts
- Cleans up serialized data from disk to prevent storage leaks
- Should be called when logical decoding is complete or when cleaning up after errors
- Requires MyReplicationSlot to be valid for cleanup operations
- Complementary function to ReorderBufferAllocate