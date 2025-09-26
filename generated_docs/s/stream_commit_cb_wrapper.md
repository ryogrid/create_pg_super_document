# stream_commit_cb_wrapper

## Location
src/backend/replication/logical/logical.c: 1470 - 1510

## Overview
A wrapper function that sets up error handling context and calls the plugin's stream_commit callback to notify about the commit of a streaming transaction in logical replication.

## Definition

```c
static void
stream_commit_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
						 XLogRecPtr commit_lsn)
```
## Detailed Description
This function serves as an internal wrapper for the stream_commit callback in PostgreSQL's logical replication system. It is called when a large streaming transaction is committed. The wrapper performs essential setup tasks including error context management, output state configuration, and validation before delegating to the actual plugin callback.

Similar to stream_abort_cb_wrapper, this function sets ctx->end_xact to true, indicating that the transaction is ending due to commit. It uses the transaction's final_lsn for error reporting but end_lsn for write location tracking, and passes the commit_lsn parameter to the plugin callback.

## Parameters / Member Variables
- `cache`: Pointer to the ReorderBuffer containing transaction data and plugin context
- `txn`: Pointer to the ReorderBufferTXN representing the streaming transaction being committed
- `commit_lsn`: XLogRecPtr indicating the LSN position where the transaction commit occurred

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
- The stream_commit_cb callback is mandatory in streaming mode - missing callback results in ERROR
- Sets up error context to provide meaningful error messages during plugin callback execution
- Configures output state including accept_writes=true and current transaction ID
- Sets ctx->end_xact = true, indicating transaction end state
- Uses txn->final_lsn for error reporting location but txn->end_lsn for write_location
- The plugin callback receives the commit_lsn as an additional parameter
- Part of PostgreSQL's logical replication streaming feature for handling large transactions
- Counterpart to stream_abort_cb_wrapper, marking successful completion of streaming transactions
- Essential for proper commit processing in streaming logical replication