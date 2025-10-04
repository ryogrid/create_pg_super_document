# filter_by_origin_cb_wrapper

## Location
[src/backend/replication/logical/logical.c:1218-1248](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L1218-L1248)

## Overview
A wrapper function that provides error handling and context management when calling logical replication output plugin filter by origin callbacks for origin-based filtering.

## Definition
```c
bool filter_by_origin_cb_wrapper(LogicalDecodingContext *ctx, RepOriginId origin_id)
```

## Detailed Description
The `filter_by_origin_cb_wrapper` function serves as an intermediary layer for filtering changes based on their replication origin in logical replication. This wrapper enables output plugins to make filtering decisions based on the origin ID of changes, which is crucial for preventing replication loops and implementing selective replication topologies. The function returns a boolean value indicating whether changes from the specified origin should be included in the logical replication stream.

This filtering mechanism is essential in multi-master replication scenarios where changes from certain origins should be filtered out to avoid infinite loops or to implement specific replication policies. The function sets `accept_writes` to false since this is a filtering operation that doesn't involve actual data output.

## Parameters / Member Variables
- `ctx`: LogicalDecodingContext containing the logical decoding state and plugin callbacks
- `origin_id`: RepOriginId representing the replication origin identifier to be evaluated for filtering

## Dependencies
- Functions called/Symbols referenced:
  - RepOriginId
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md)
  - [LogicalErrorCallbackState](../L/LogicalErrorCallbackState.md)
  - [output_plugin_error_callback](../o/output_plugin_error_callback.md)
- Called from (representative examples):
  - [FilterByOrigin](../F/FilterByOrigin.md)

## Notes and Other Information
- This function is only called when `ctx->fast_forward` is false, ensuring it's not used during fast-forward mode
- Returns a boolean value indicating whether changes from the specified origin should be processed
- Sets `ctx->accept_writes = false` since this is a filtering operation, not a data output operation
- Uses `InvalidXLogRecPtr` for the report location since origin filtering doesn't have a specific LSN context
- Sets `ctx->end_xact = false` to indicate this is not a transaction end event
- Critical for preventing replication loops in multi-master setups
- Enables selective replication where only changes from specific origins are replicated
- The origin_id parameter corresponds to replication origins configured in the system
- Error context management ensures meaningful error messages if the plugin's filter callback fails
- Part of PostgreSQL's logical replication origin tracking and filtering infrastructure

## Simplified Source

```c
bool
filter_by_origin_cb_wrapper(LogicalDecodingContext *ctx, RepOriginId origin_id)
{
    Assert(!ctx->fast_forward);

    // Set up error context for meaningful error reporting
    LogicalErrorCallbackState state;
    ErrorContextCallback errcallback;

    state.ctx = ctx;
    state.callback_name = "filter_by_origin";
    state.report_location = InvalidXLogRecPtr;
    errcallback.callback = output_plugin_error_callback;
    errcallback.arg = (void *) &state;
    errcallback.previous = error_context_stack;
    error_context_stack = &errcallback;

    // Configure output state for filtering operation
    ctx->accept_writes = false;
    ctx->end_xact = false;

    // Call the actual plugin filter callback
    bool ret = ctx->callbacks.filter_by_origin_cb(ctx, origin_id);

    // Restore error context
    error_context_stack = errcallback.previous;

    return ret;
}
```