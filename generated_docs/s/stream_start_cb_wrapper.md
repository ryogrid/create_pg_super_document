# stream_start_cb_wrapper

## Location
[src/backend/replication/logical/logical.c:1286-1334](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L1286-L1334)

## Overview
A wrapper function that sets up error handling context and calls the plugin's stream_start callback to notify about the start of a streaming transaction in logical replication.

## Definition

```c
static void
stream_start_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
						XLogRecPtr first_lsn)
```
## Detailed Description
This function serves as an internal wrapper for the stream_start callback in PostgreSQL's logical replication system. It is called when a large transaction begins streaming its changes before the transaction commits. The wrapper performs essential setup tasks including error context management, output state configuration, and validation before delegating to the actual plugin callback.

The function ensures proper error reporting by setting up an error context stack that will provide meaningful error messages if the plugin callback fails. It also configures the logical decoding context for write operations and updates location tracking for replication progress monitoring.

## Parameters / Member Variables
- : Pointer to the ReorderBuffer containing transaction data and plugin context
- : Pointer to the ReorderBufferTXN representing the streaming transaction
- : XLogRecPtr indicating the LSN position where streaming begins

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
- Only called when streaming is supported (ctx->streaming must be true)
- Fast-forward mode is not compatible with streaming (Assert(!ctx->fast_forward))
- The stream_start_cb callback is mandatory in streaming mode - missing callback results in ERROR
- Sets up error context to provide meaningful error messages during plugin callback execution
- Configures output state including accept_writes=true and current transaction ID
- Updates write_location for replication progress tracking
- Part of PostgreSQL's logical replication streaming feature for handling large transactions