# stream_truncate_cb_wrapper

## Location
[src/backend/replication/logical/logical.c:1601-1647](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L1601-L1647)

## Overview
A wrapper function that safely invokes the optional output plugin's stream_truncate_cb callback during logical replication streaming for handling TRUNCATE operations on multiple relations.

## Definition

```c
static void
stream_truncate_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
						   int nrelations, Relation relations[],
						   ReorderBufferChange *change)
```
## Detailed Description
This function serves as a protective wrapper around the output plugin's optional stream_truncate_cb callback in PostgreSQL's logical replication streaming mechanism. It handles TRUNCATE operations that can affect multiple relations simultaneously within a streaming transaction context.

The function is responsible for managing the logical decoding context state, setting up proper error handling, and safely delegating to the plugin's callback when available. TRUNCATE operations are special because they can affect multiple tables in a single statement, which is reflected in the parameters.

Key responsibilities include:
1. Checking if the optional stream_truncate_cb callback is available
2. Setting up error context for better error reporting during truncate operations
3. Configuring the logical decoding context for write operations
4. Managing the write location for client progress tracking
5. Safely invoking the plugin's callback with proper error handling

## Parameters / Member Variables
- : ReorderBuffer instance containing the private logical decoding context
- : ReorderBufferTXN representing the current transaction being processed
- : Integer count of the number of relations being truncated
- : Array of Relation objects representing the tables being truncated
- : ReorderBufferChange containing the specific truncate change being streamed

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBuffer](../R/ReorderBuffer.md)
  - [ReorderBufferTXN](../R/ReorderBufferTXN.md)
  - [ReorderBufferChange](../R/ReorderBufferChange.md)
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md)
  - LogicalErrorCallbackState
  - output_plugin_error_callback
- Called from (representative examples):
  - StartupDecodingContext

## Notes and Other Information
- This function is only called when streaming is supported (ctx->streaming must be true)
- The function asserts that fast_forward mode is not active
- This callback is optional - the function returns early if not provided by the plugin
- TRUNCATE operations can affect multiple relations simultaneously, hence the nrelations parameter and relations array
- The function properly manages error context stack to ensure cleanup on both success and failure paths
- Location tracking is updated to allow clients to provide up-to-date progress information
- Unlike single-table operations, truncate can be more complex due to its multi-relation nature