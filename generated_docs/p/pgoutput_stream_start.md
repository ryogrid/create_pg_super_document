# pgoutput_stream_start

## Location
[src/backend/replication/pgoutput/pgoutput.c:1783-1814](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L1783-L1814)

## Overview
pgoutput_stream_start is a callback function that handles the start of streaming for large transactions in PostgreSQL logical replication, outputting stream start messages to the replication protocol.

## Definition

```c
static void
pgoutput_stream_start(struct LogicalDecodingContext *ctx,
					  ReorderBufferTXN *txn)
```
## Detailed Description
pgoutput_stream_start is a callback function in the pgoutput logical replication output plugin that handles the START STREAM event for large transactions that are being streamed in chunks rather than being buffered entirely in memory. When a transaction is large enough to trigger streaming mode, this function is called at the beginning of each stream chunk. It writes a stream start message to the logical replication protocol, handles replication origin information appropriately (only sending it for the first stream of a transaction), and sets internal state to indicate that streaming is active.

## Parameters / Member Variables
- `*ctx`: Logical decoding context containing output plugin state and configuration
- `*txn`: ReorderBufferTXN structure representing the transaction being streamed
## Dependencies
- Functions called/Symbols referenced:
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md)
  - [ReorderBufferTXN](../R/ReorderBufferTXN.md)
  - [PGOutputData](../P/PGOutputData.md)
  - InvalidRepOriginId
  - rbtxn_is_streamed
  - [OutputPluginPrepareWrite](../O/OutputPluginPrepareWrite.md)
  - [logicalrep_write_stream_start](../l/logicalrep_write_stream_start.md)
  - [send_repl_origin](../s/send_repl_origin.md)
  - [OutputPluginWrite](../O/OutputPluginWrite.md)
- Called from (representative examples):
  - [_PG_output_plugin_init](../P/_PG_output_plugin_init.md) (registered as callback)

## Notes and Other Information
- This is a static function, only accessible within the pgoutput.c file
- Part of the streaming transaction feature that allows processing of large transactions without excessive memory usage
- Includes an assertion to prevent nesting of streaming transactions
- Only sends replication origin information for the first stream of a transaction to avoid redundancy
- Sets the in_streaming flag in PGOutputData to track streaming state
- Uses the logical replication protocol message format for communicating with subscribers
- Critical for handling large transactions efficiently in logical replication scenarios

## Simplified Source

```c
static void
pgoutput_stream_start(struct LogicalDecodingContext *ctx, ReorderBufferTXN *txn) {
    PGOutputData *data = (PGOutputData *) ctx->output_plugin_private;

    // Determine if we should send replication origin info
    // Only send for first stream of this transaction
    bool send_replication_origin = (txn->origin_id != InvalidRepOriginId) &&
                                  !rbtxn_is_streamed(txn);

    // Write stream start message to output
    OutputPluginPrepareWrite(ctx, !send_replication_origin);
    logicalrep_write_stream_start(ctx->out, txn->xid, !rbtxn_is_streamed(txn));

    // Send origin info if needed
    send_repl_origin(ctx, txn->origin_id, InvalidXLogRecPtr, send_replication_origin);

    // Commit the write and mark streaming as active
    OutputPluginWrite(ctx, true);
    data->in_streaming = true;
}
```