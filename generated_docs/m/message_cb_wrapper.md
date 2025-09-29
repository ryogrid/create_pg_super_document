# message_cb_wrapper

## Location
[src/backend/replication/logical/logical.c:1249-1285](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L1249-L1285)

## Overview
A wrapper function that provides error handling and context management when calling logical replication output plugin message callbacks for logical messages.

## Definition
```c
static void message_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
                              XLogRecPtr message_lsn, bool transactional,
                              const char *prefix, Size message_size, const char *message)
```

## Detailed Description
The `message_cb_wrapper` function serves as an intermediary layer for handling logical messages in logical replication. Logical messages are custom messages that can be written to the WAL and decoded by logical replication, allowing applications to send custom data through the replication stream. This wrapper provides the same error context management and output state handling as other callback wrappers, but is specifically designed for logical messages that can be either transactional or non-transactional.

The function includes a null check for the message callback since message handling is optional in output plugins. It properly handles both transactional and non-transactional messages, setting the appropriate transaction context based on whether a transaction is associated with the message.

## Parameters / Member Variables
- `cache`: ReorderBuffer instance containing the logical decoding state and private plugin data
- `txn`: ReorderBufferTXN representing the transaction context (can be NULL for non-transactional messages)
- `message_lsn`: XLogRecPtr indicating the LSN where the message was written in the WAL
- `transactional`: Boolean flag indicating whether the message is part of a transaction
- `prefix`: String prefix identifying the message type or source
- `message_size`: Size of the message content in bytes
- `message`: Pointer to the actual message content

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBuffer](../R/ReorderBuffer.md)
  - [ReorderBufferTXN](../R/ReorderBufferTXN.md)
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md)
  - [LogicalErrorCallbackState](../L/LogicalErrorCallbackState.md)
  - [output_plugin_error_callback](../o/output_plugin_error_callback.md)
- Called from (representative examples):
  - [StartupDecodingContext](../S/StartupDecodingContext.md)

## Notes and Other Information
- This function is only called when `ctx->fast_forward` is false, ensuring it's not used during fast-forward mode
- Includes a null check for `ctx->callbacks.message_cb` since message callbacks are optional in output plugins
- Returns early if no message callback is registered, making it safe to call even when the plugin doesn't support logical messages
- Sets `ctx->accept_writes = true` to enable output plugin writing
- Handles both transactional and non-transactional messages appropriately
- Sets `ctx->write_xid` to the transaction ID if available, or `InvalidTransactionId` for non-transactional messages
- Uses the message's LSN for both error reporting and write location tracking
- Sets `ctx->end_xact = false` to indicate this is not a transaction end event
- Logical messages enable custom application data to be replicated alongside regular DML operations
- The prefix parameter allows applications to categorize or identify different types of logical messages
- Message size and content are passed directly to the plugin for processing

## Simplified Source

```c
static void message_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
                              XLogRecPtr message_lsn, bool transactional,
                              const char *prefix, Size message_size, const char *message)
{
    LogicalDecodingContext *ctx = cache->private_data;
    LogicalErrorCallbackState state;
    ErrorContextCallback errcallback;

    Assert(!ctx->fast_forward);

    // Skip if no message callback is registered
    if (ctx->callbacks.message_cb == NULL)
        return;

    // Set up error handling context
    state.ctx = ctx;
    state.callback_name = "message";
    state.report_location = message_lsn;
    errcallback.callback = output_plugin_error_callback;
    errcallback.arg = (void *) &state;
    errcallback.previous = error_context_stack;
    error_context_stack = &errcallback;

    // Configure output state
    ctx->accept_writes = true;
    ctx->write_xid = txn != NULL ? txn->xid : InvalidTransactionId;
    ctx->write_location = message_lsn;
    ctx->end_xact = false;

    // Call the actual plugin message callback
    ctx->callbacks.message_cb(ctx, txn, message_lsn, transactional, prefix,
                             message_size, message);

    // Restore error context
    error_context_stack = errcallback.previous;
}
```