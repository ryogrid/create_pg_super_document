# stream_prepare_cb_wrapper

## Location
[src/backend/replication/logical/logical.c:1425-1469](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L1425-L1469)

## Overview
A wrapper function that sets up error handling context and calls the plugin's stream_prepare callback to notify about the preparation phase of a streaming two-phase commit transaction in logical replication.

## Definition

```c
static void
stream_prepare_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
						  XLogRecPtr prepare_lsn)
```
## Detailed Description
This function serves as an internal wrapper for the stream_prepare callback in PostgreSQL's logical replication system. It is specifically called during the prepare phase of two-phase commit transactions that are being streamed. The wrapper performs essential setup tasks including error context management, output state configuration, and validation before delegating to the actual plugin callback.

This function requires both streaming and two-phase commit support to be enabled (ctx->streaming and ctx->twophase must both be true). It sets ctx->end_xact to true and uses the transaction's final_lsn for error reporting while using end_lsn for write location tracking.

## Parameters / Member Variables
- `cache`: Pointer to the ReorderBuffer containing transaction data and plugin context
- `txn`: Pointer to the ReorderBufferTXN representing the streaming two-phase commit transaction being prepared
- `prepare_lsn`: XLogRecPtr indicating the LSN position where the prepare operation occurred

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBuffer](../R/ReorderBuffer.md)
  - [ReorderBufferTXN](../R/ReorderBufferTXN.md)
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md)
  - [LogicalErrorCallbackState](../L/LogicalErrorCallbackState.md)
  - [output_plugin_error_callback](../o/output_plugin_error_callback.md)
  - ereport (error reporting)
- Called from (representative examples):
  - [StartupDecodingContext](../S/StartupDecodingContext.md)

## Notes and Other Information
- Only called when both streaming and two-phase commits are supported (ctx->streaming and ctx->twophase must be true)
- Fast-forward mode is not compatible with streaming (Assert(!ctx->fast_forward))
- The stream_prepare_cb callback is mandatory in streaming mode with two-phase commits - missing callback results in ERROR
- Sets up error context to provide meaningful error messages during plugin callback execution
- Configures output state including accept_writes=true and current transaction ID
- Sets ctx->end_xact = true, indicating transaction end state
- Uses txn->final_lsn for error reporting location but txn->end_lsn for write_location
- The plugin callback receives the prepare_lsn as an additional parameter
- Part of PostgreSQL's logical replication streaming feature for two-phase commit transactions
- Essential for proper handling of prepared transactions in streaming logical replication
- Enables logical decoding plugins to handle the prepare phase of distributed transactions