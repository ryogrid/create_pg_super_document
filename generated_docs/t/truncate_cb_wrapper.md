# truncate_cb_wrapper

## Location
[src/backend/replication/logical/logical.c:1144-1185](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L1144-L1185)

## Overview
A wrapper function that provides error handling and context management when calling logical replication output plugin truncate callbacks during logical decoding.

## Definition
```c
static void truncate_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
                               int nrelations, Relation relations[], ReorderBufferChange *change)
```

## Detailed Description
The `truncate_cb_wrapper` function serves as an intermediary layer for handling TRUNCATE operations in logical replication. It provides the same error context management and output state handling as other callback wrappers, but is specifically designed for truncate operations that can affect multiple relations simultaneously. The function includes a safety check to ensure the truncate callback is available before attempting to call it, as truncate callbacks are optional in output plugins.

Like other callback wrappers, it establishes proper error context, manages output state, and ensures that LSN information is correctly propagated. The key difference is that truncate operations can affect multiple tables in a single operation, hence the array of relations parameter.

## Parameters / Member Variables
- `cache`: ReorderBuffer instance containing the logical decoding state and private plugin data
- `txn`: ReorderBufferTXN representing the current transaction being processed
- `nrelations`: Number of relations (tables) affected by the truncate operation
- `relations[]`: Array of Relation objects representing the tables being truncated
- `change`: ReorderBufferChange containing the truncate operation details

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBuffer](../R/ReorderBuffer.md)
  - [ReorderBufferTXN](../R/ReorderBufferTXN.md)
  - [ReorderBufferChange](../R/ReorderBufferChange.md)
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md)
  - [LogicalErrorCallbackState](../L/LogicalErrorCallbackState.md)
  - [output_plugin_error_callback](../o/output_plugin_error_callback.md)
- Called from (representative examples):
  - [StartupDecodingContext](../S/StartupDecodingContext.md)

## Notes and Other Information
- This function is only called when `ctx->fast_forward` is false, ensuring it's not used during fast-forward mode
- Includes a null check for `ctx->callbacks.truncate_cb` since truncate callbacks are optional in output plugins
- Returns early if no truncate callback is registered, making it safe to call even when the plugin doesn't support truncate operations
- Sets `ctx->accept_writes = true` to enable output plugin writing
- Updates `ctx->write_location` with the change's LSN for client reply coordination
- Manages error context stack to provide meaningful error messages if the plugin callback fails
- Handles multiple relations in a single operation, which is unique to TRUNCATE among DML operations
- Sets `ctx->end_xact = false` to indicate this is not a transaction end event

## Simplified Source

```c
// Simplified version of truncate_cb_wrapper
static void truncate_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
                               int nrelations, Relation relations[],
                               ReorderBufferChange *change) {
    LogicalDecodingContext *ctx = cache->private_data;

    // Skip if in fast-forward mode
    if (ctx->fast_forward)
        return;

    // Return early if plugin doesn't support truncate operations
    if (!ctx->callbacks.truncate_cb)
        return;

    // Set up error context for better error reporting
    LogicalErrorCallbackState state;
    ErrorContextCallback errcallback;
    state.ctx = ctx;
    state.callback_name = "truncate";
    state.report_location = change->lsn;
    errcallback.callback = output_plugin_error_callback;
    errcallback.arg = (void *) &state;
    errcallback.previous = error_context_stack;
    error_context_stack = &errcallback;

    // Configure output state for the plugin
    ctx->accept_writes = true;
    ctx->write_xid = txn->xid;
    ctx->write_location = change->lsn;
    ctx->end_xact = false;

    // Call the plugin's truncate callback
    ctx->callbacks.truncate_cb(ctx, txn, nrelations, relations, change);

    // Restore previous error context
    error_context_stack = errcallback.previous;
}
```

Key simplifications made:
- Added early returns with clear comments for skip conditions
- Grouped related error context setup code together
- Grouped output state configuration together
- Added descriptive comments explaining each major section
- Simplified error context management while preserving functionality