# stream_message_cb_wrapper

## Location
[src/backend/replication/logical/logical.c:1560-1600](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logical.c#L1560-L1600)

## Overview
A wrapper function that safely invokes the optional output plugin's stream_message_cb callback during logical replication streaming for handling transactional and non-transactional messages.

## Definition

```c
static void
stream_message_cb_wrapper(ReorderBuffer *cache, ReorderBufferTXN *txn,
						  XLogRecPtr message_lsn, bool transactional,
						  const char *prefix, Size message_size, const char *message)
```
## Detailed Description
This function serves as a protective wrapper around the output plugin's optional stream_message_cb callback in PostgreSQL's logical replication streaming mechanism. Unlike other stream callbacks, this one is optional and the function returns early if the callback is not provided by the plugin.

The function handles both transactional and non-transactional messages that are part of the logical replication stream. It properly manages the logical decoding context state, sets up error handling, and delegates to the plugin's callback when available.

Key responsibilities include:
1. Checking if the optional stream_message_cb callback is available
2. Setting up error context for better error reporting
3. Configuring the logical decoding context for write operations
4. Handling both transactional and non-transactional message scenarios
5. Safely invoking the plugin's callback with proper error handling

## Parameters / Member Variables
- : ReorderBuffer instance containing the private logical decoding context
- : ReorderBufferTXN representing the current transaction (can be NULL for non-transactional messages)
- : XLogRecPtr indicating the LSN where the message was logged
- : Boolean flag indicating whether the message is part of a transaction
- : String prefix associated with the message
- : Size of the message content
- : The actual message content

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
- This function is only called when streaming is supported (ctx->streaming must be true)
- The function asserts that fast_forward mode is not active
- Unlike other stream callbacks, this callback is optional - the function returns early if not provided
- Handles both transactional and non-transactional messages appropriately
- The txn parameter can be NULL for non-transactional messages, and the function handles this case by setting write_xid to InvalidTransactionId
- Manages error context stack properly to ensure cleanup on both success and failure paths