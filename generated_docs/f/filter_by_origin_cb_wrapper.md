# filter_by_origin_cb_wrapper

## Location
src/backend/replication/logical/logical.c: 1218 - 1248

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
  - LogicalDecodingContext
  - LogicalErrorCallbackState
  - output_plugin_error_callback
- Called from (representative examples):
  - FilterByOrigin

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