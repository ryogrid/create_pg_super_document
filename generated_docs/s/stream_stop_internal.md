# stream_stop_internal

## Location
[src/backend/replication/logical/worker.c:1605-1627](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L1605-L1627)

## Overview
Finalizes the processing of a streaming transaction by serializing subtransaction information, closing the stream spool file, committing the per-stream transaction, and resetting the streaming context.

## Definition

```c
void
stream_stop_internal(TransactionId xid)
```
## Detailed Description
This function performs the cleanup and finalization tasks required when a streaming transaction ends. It must be called after stream_start_internal has been invoked to properly clean up the streaming transaction state. The function handles four critical tasks:

1. **Subtransaction Serialization**: Writes subtransaction information to persistent storage for the top-level transaction
2. **File Management**: Closes the stream messages spool file used for buffering changes
3. **Transaction Commit**: Commits the per-stream transaction that was started during stream processing
4. **Memory Cleanup**: Resets the LogicalStreamingContext to free memory allocated during streaming

This function ensures that all streaming transaction state is properly cleaned up and committed, maintaining data consistency in logical replication.

## Parameters / Member Variables
- `xid`: TransactionId of the top-level streaming transaction being finalized
## Dependencies
- Functions called/Symbols referenced:
  - [subxact_info_write](subxact_info_write.md)
  - [stream_close_file](stream_close_file.md)
  - [IsTransactionState](../I/IsTransactionState.md)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md)
  - [MemoryContextReset](../M/MemoryContextReset.md)
- Called from:
  - [apply_handle_stream_stop](../a/apply_handle_stream_stop.md)
  - [stream_open_and_write_change](stream_open_and_write_change.md)

## Notes and Other Information
- Must be called after stream_start_internal has been invoked
- Includes assertion to verify we are in a valid transaction state before committing
- The function commits the per-stream transaction that was started during stream_start_internal
- Memory context reset helps prevent memory leaks in long-running replication workers
- Part of the logical replication streaming transaction cleanup protocol