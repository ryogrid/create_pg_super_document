# update_progress_txn_cb_wrapper

## Location
[src/backend/replication/logical/logical.c:1648-1694](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L1648-L1694)

## Overview
A wrapper function that handles progress updates during logical replication transaction processing, notifying clients of replication progress without allowing output writes.

## Definition

```c
static void
update_progress_txn_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
							   XLogRecPtr lsn)
```
## Detailed Description
This function serves as a wrapper for updating progress during logical replication transaction processing. Unlike other callback wrappers, this function doesn't delegate to a plugin-specific callback but instead calls the core OutputPluginUpdateProgress function to notify clients about replication progress.

The function is designed to provide progress updates during long-running transactions or when significant amounts of data have been processed. It maintains the logical decoding context state and ensures proper error handling while explicitly disallowing output writes (ctx->accept_writes = false) since this is purely a progress notification mechanism.

Key responsibilities include:
1. Setting up error context for progress update operations
2. Configuring the logical decoding context with write restrictions
3. Updating the write location for client progress tracking
4. Calling the core progress update mechanism
5. Managing error context stack properly

## Parameters / Member Variables
- `*cache`: ReorderBuffer instance containing the private logical decoding context
- `*txn`: ReorderBufferTXN representing the current transaction being processed
- `lsn`: XLogRecPtr indicating the current LSN position for progress reporting
## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBuffer](../R/ReorderBuffer.md)
  - [ReorderBufferTXN](../R/ReorderBufferTXN.md)
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md)
  - [LogicalErrorCallbackState](../L/LogicalErrorCallbackState.md)
  - [output_plugin_error_callback](../o/output_plugin_error_callback.md)
  - [OutputPluginUpdateProgress](../O/OutputPluginUpdateProgress.md)
- Called from (representative examples):
  - [StartupDecodingContext](../S/StartupDecodingContext.md)

## Notes and Other Information
- This function is used for progress tracking rather than data output
- The function asserts that fast_forward mode is not active
- Unlike other wrappers, this explicitly sets ctx->accept_writes to false, preventing output during progress updates
- The function calls OutputPluginUpdateProgress(ctx, false) to send progress notifications to clients
- Progress updates help clients track replication lag and processing status during long-running transactions
- Manages error context stack properly to ensure cleanup on both success and failure paths
- The write_location is updated to reflect the current processing position for accurate progress reporting

## Simplified Source

```c
// Simplified version of update_progress_txn_cb_wrapper
static void update_progress_txn_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
                                          XLogRecPtr lsn) {
    LogicalDecodingContext *ctx = cache->private_data;

    // Set up error context for progress updates
    LogicalErrorCallbackState state;
    ErrorContextCallback errcallback;
    state.ctx = ctx;
    state.callback_name = "update_progress_txn";
    state.report_location = lsn;
    errcallback.callback = output_plugin_error_callback;
    errcallback.arg = (void *) &state;
    errcallback.previous = error_context_stack;
    error_context_stack = &errcallback;

    // Configure output state for progress updates (no writes allowed)
    ctx->accept_writes = false;
    ctx->write_xid = txn->xid;
    ctx->write_location = lsn;
    ctx->end_xact = false;

    // Send progress update to clients
    OutputPluginUpdateProgress(ctx, false);

    // Restore error context
    error_context_stack = errcallback.previous;
}
```

Key simplifications made:
- Removed assertion check for clarity
- Grouped error context setup together
- Grouped output state configuration together
- Added descriptive comments for each section
- Emphasized that this is for progress tracking, not data output