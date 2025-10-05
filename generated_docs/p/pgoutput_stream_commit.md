# pgoutput_stream_commit

## Location
[src/backend/replication/pgoutput/pgoutput.c:1869-1896](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L1869-L1896)

## Overview
This function notifies downstream to apply a streamed transaction along with all its subtransactions by writing a stream commit message to the logical replication output stream.

## Definition

```c
static void
pgoutput_stream_commit(struct LogicalDecodingContext *ctx,
					   ReorderBufferTXN *txn,
					   XLogRecPtr commit_lsn)
```
## Detailed Description
The  function is a callback used in PostgreSQL's logical replication system to handle the commit of streamed transactions. It operates as part of the pgoutput plugin's transaction streaming functionality. The function ensures that the transaction commit happens outside of the streaming block while maintaining the transaction's streamed status. It sends a stream commit message to the downstream subscriber and performs cleanup of the relation synchronization cache.

## Parameters / Member Variables
- `*ctx`: LogicalDecodingContext pointer containing the decoding context and output plugin state
- `*txn`: ReorderBufferTXN pointer representing the transaction being committed
- `commit_lsn`: XLogRecPtr specifying the WAL location where the transaction was committed
## Dependencies
- Functions called/Symbols referenced:
  - [OutputPluginUpdateProgress](../O/OutputPluginUpdateProgress.md)
  - [OutputPluginPrepareWrite](../O/OutputPluginPrepareWrite.md)
  - [logicalrep_write_stream_commit](../l/logicalrep_write_stream_commit.md)
  - [OutputPluginWrite](../O/OutputPluginWrite.md)
  - [cleanup_rel_sync_cache](../c/cleanup_rel_sync_cache.md)
  - rbtxn_is_streamed
- Called from (representative examples):
  - [_PG_output_plugin_init](../P/_PG_output_plugin_init.md) (registered as callback)

## Notes and Other Information
- The function includes assertions to ensure it's called outside of a streaming block (!data->in_streaming) and that the transaction is marked as streamed (rbtxn_is_streamed(txn))
- This is a static function specific to the pgoutput plugin implementation
- The function is registered as a callback during plugin initialization in _PG_output_plugin_init
- After writing the commit message, it cleans up the relation synchronization cache for the committed transaction
- The commit_lsn parameter allows downstream systems to track the exact WAL position of the commit

## Simplified Source

```c
static void
pgoutput_stream_commit(struct LogicalDecodingContext *ctx,
                      ReorderBufferTXN *txn,
                      XLogRecPtr commit_lsn) {
    // Update replication progress
    OutputPluginUpdateProgress(ctx, false);

    // Write stream commit message to output
    OutputPluginPrepareWrite(ctx, true);
    logicalrep_write_stream_commit(ctx->out, txn, commit_lsn);
    OutputPluginWrite(ctx, true);

    // Clean up relation sync cache for committed transaction
    cleanup_rel_sync_cache(txn->xid, true);
}
```