# stream_stop_internal

## Location
src/backend/replication/logical/worker.c: 1605 - 1627

## Overview
Finalizes the processing of a streaming transaction by serializing subtransaction information, closing the stream spool file, committing the per-stream transaction, and resetting the streaming context.

## Definition


## Detailed Description
This function performs the cleanup and finalization tasks required when a streaming transaction ends. It must be called after stream_start_internal has been invoked to properly clean up the streaming transaction state. The function handles four critical tasks:

1. **Subtransaction Serialization**: Writes subtransaction information to persistent storage for the top-level transaction
2. **File Management**: Closes the stream messages spool file used for buffering changes
3. **Transaction Commit**: Commits the per-stream transaction that was started during stream processing
4. **Memory Cleanup**: Resets the LogicalStreamingContext to free memory allocated during streaming

This function ensures that all streaming transaction state is properly cleaned up and committed, maintaining data consistency in logical replication.

## Parameters / Member Variables
- : TransactionId of the top-level streaming transaction being finalized

## Dependencies
- Functions called/Symbols referenced:
  - subxact_info_write
  - stream_close_file
  - IsTransactionState
  - CommitTransactionCommand
  - MemoryContextReset
- Called from:
  - apply_handle_stream_stop
  - stream_open_and_write_change

## Notes and Other Information
- Must be called after stream_start_internal has been invoked
- Includes assertion to verify we are in a valid transaction state before committing
- The function commits the per-stream transaction that was started during stream_start_internal
- Memory context reset helps prevent memory leaks in long-running replication workers
- Part of the logical replication streaming transaction cleanup protocol