# FreeDecodingContext

## Location
[src/backend/replication/logical/logical.c:696-710](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L696-L710)

## Overview
Frees a previously allocated logical decoding context, properly cleaning up all associated resources and invoking shutdown callbacks.

## Definition
void FreeDecodingContext(LogicalDecodingContext *ctx)

## Detailed Description
This function performs comprehensive cleanup of a LogicalDecodingContext by systematically freeing all allocated resources in the proper order. It first invokes the output plugin's shutdown callback if one exists, then proceeds to free the reorder buffer, snapshot builder, WAL reader, and finally the entire memory context. This ensures that all resources are properly released and any necessary cleanup operations are performed before the context is destroyed.

The cleanup sequence is:
1. Call the output plugin's shutdown callback if present
2. Free the reorder buffer (ReorderBufferFree)
3. Free the snapshot builder (FreeSnapshotBuilder) 
4. Free the WAL reader (XLogReaderFree)
5. Delete the entire memory context

## Parameters / Member Variables
- : LogicalDecodingContext pointer to the decoding context to be freed

## Dependencies
- Functions called/Symbols referenced:
  - shutdown_cb_wrapper
  - [ReorderBufferFree](../R/ReorderBufferFree.md)
  - [FreeSnapshotBuilder](FreeSnapshotBuilder.md)
  - [XLogReaderFree](../X/XLogReaderFree.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
- Called from (representative examples):
  - LogicalReplicationSlotHasPendingWal
  - [LogicalSlotAdvanceAndCheckSnapState](../L/LogicalSlotAdvanceAndCheckSnapState.md)
  - [pg_logical_slot_get_changes_guts](../p/pg_logical_slot_get_changes_guts.md)
  - [create_logical_replication_slot](../c/create_logical_replication_slot.md)
  - [CreateReplicationSlot](../C/CreateReplicationSlot.md)
  - [StartLogicalReplication](../S/StartLogicalReplication.md)

## Notes and Other Information
- This function should always be called to properly clean up a LogicalDecodingContext
- The shutdown callback allows output plugins to perform any necessary cleanup before the context is destroyed
- Memory context deletion ensures all memory allocated within the context is freed
- Used extensively throughout the logical replication system when decoding operations complete or encounter errors