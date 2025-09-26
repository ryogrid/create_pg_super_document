# stream_stop_cb_wrapper

## Location
src/backend/replication/logical/logical.c: 1335 - 1383

## Overview
A wrapper function that sets up error handling context and calls the plugin's stream_stop callback to notify about the end of a streaming transaction in logical replication.

## Definition

```c
static void
stream_stop_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
					   XLogRecPtr last_lsn)
```
## Detailed Description
This function serves as an internal wrapper for the stream_stop callback in PostgreSQL's logical replication system. It is called when a large transaction finishes streaming its changes, typically before the transaction commits. The wrapper performs essential setup tasks including error context management, output state configuration, and validation before delegating to the actual plugin callback.

Similar to stream_start_cb_wrapper, this function ensures proper error reporting by setting up an error context stack and configures the logical decoding context for write operations. It updates location tracking to the final LSN position of the streamed transaction.

## Parameters / Member Variables
- `cache`: Pointer to the ReorderBuffer containing transaction data and plugin context
- `txn`: Pointer to the ReorderBufferTXN representing the streaming transaction that is ending
- `last_lsn`: XLogRecPtr indicating the LSN position where streaming ends

## Dependencies
- Functions called/Symbols referenced:
  - ReorderBuffer
  - ReorderBufferTXN
  - LogicalDecodingContext
  - LogicalErrorCallbackState
  - output_plugin_error_callback
  - ereport (error reporting)
- Called from (representative examples):
  - StartupDecodingContext

## Notes and Other Information
- Only called when streaming is supported (ctx->streaming must be true)
- Fast-forward mode is not compatible with streaming (Assert(!ctx->fast_forward))
- The stream_stop_cb callback is mandatory in streaming mode - missing callback results in ERROR
- Sets up error context to provide meaningful error messages during plugin callback execution
- Configures output state including accept_writes=true and current transaction ID
- Updates write_location to the last LSN for replication progress tracking
- Counterpart to stream_start_cb_wrapper, marking the end of transaction streaming
- Part of PostgreSQL's logical replication streaming feature for handling large transactions