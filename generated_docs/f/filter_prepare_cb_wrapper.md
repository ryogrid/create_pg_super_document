# filter_prepare_cb_wrapper

## Location
[src/backend/replication/logical/logical.c:1186-1217](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L1186-L1217)

## Overview
A wrapper function that provides error handling and context management when calling logical replication output plugin filter prepare callbacks for two-phase commit transactions.

## Definition
```c
bool filter_prepare_cb_wrapper(LogicalDecodingContext *ctx, TransactionId xid,
                              const char *gid)
```

## Detailed Description
The `filter_prepare_cb_wrapper` function serves as an intermediary layer for filtering prepared transactions in logical replication's two-phase commit support. Unlike other callback wrappers, this function returns a boolean value indicating whether the prepared transaction should be decoded and sent to the output plugin. It establishes proper error context while allowing output plugins to make filtering decisions about prepared transactions based on the transaction ID and global identifier.

This wrapper is specifically designed for the prepare phase of two-phase commit transactions, where the plugin can decide whether to include a particular prepared transaction in the logical replication stream. The function sets `accept_writes` to false since this is a filtering operation that doesn't involve actual data output.

## Parameters / Member Variables
- `ctx`: LogicalDecodingContext containing the logical decoding state and plugin callbacks
- `xid`: TransactionId of the transaction being prepared
- `gid`: Global identifier (GID) string for the prepared transaction, can be NULL

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md)
  - [LogicalErrorCallbackState](../L/LogicalErrorCallbackState.md)
  - [output_plugin_error_callback](../o/output_plugin_error_callback.md)
- Called from (representative examples):
  - [FilterPrepare](../F/FilterPrepare.md)

## Notes and Other Information
- This function is only called when `ctx->fast_forward` is false, ensuring it's not used during fast-forward mode
- Returns a boolean value indicating whether the prepared transaction should be processed
- Sets `ctx->accept_writes = false` since this is a filtering operation, not a data output operation
- Uses `InvalidXLogRecPtr` for the report location since prepare filtering doesn't have a specific LSN context
- Sets `ctx->end_xact = false` to indicate this is not a transaction end event
- Part of PostgreSQL's two-phase commit support in logical replication
- The GID parameter may be NULL for transactions that weren't explicitly prepared with a global identifier
- Error context management ensures meaningful error messages if the plugin's filter callback fails