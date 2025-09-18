# pgoutput_commit_prepared_txn

## Location
[src/backend/replication/pgoutput/pgoutput.c:673-686](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L673-L686)

## Overview
Handles the COMMIT PREPARED callback for two-phase commit transactions in logical replication, finalizing previously prepared transactions.

## Definition
static void pgoutput_commit_prepared_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr commit_lsn)

## Detailed Description
This function is the commit prepared callback for the pgoutput logical replication plugin, implementing the final phase of two-phase commit protocol. It is called when a previously prepared transaction is committed and sends a COMMIT PREPARED message to subscribers. This completes the two-phase commit process by instructing subscribers to finalize transactions that were previously prepared. The function updates replication progress and ensures that the commit prepared information is properly transmitted to maintain consistency across the replication topology.

## Parameters / Member Variables
- ctx: LogicalDecodingContext pointer containing the output stream and plugin context
- txn: ReorderBufferTXN pointer representing the prepared transaction being committed
- commit_lsn: XLogRecPtr indicating the LSN where the prepared transaction was committed

## Dependencies
- Functions called/Symbols referenced:
  - [OutputPluginUpdateProgress](../O/OutputPluginUpdateProgress.md)
  - [OutputPluginPrepareWrite](../O/OutputPluginPrepareWrite.md)
  - logicalrep_write_commit_prepared
  - [OutputPluginWrite](../O/OutputPluginWrite.md)
- Called from (representative examples):
  - [_PG_output_plugin_init](../P/_PG_output_plugin_init.md) (registered as commit prepared callback)

## Notes and Other Information
- This is a static function internal to the pgoutput plugin
- Completes the two-phase commit protocol sequence started by pgoutput_begin_prepare_txn
- Always updates progress (second parameter to OutputPluginUpdateProgress is false)
- Simpler than regular commit callback as prepared transactions have already been validated
- Works in conjunction with pgoutput_begin_prepare_txn and pgoutput_prepare_txn
- Essential for maintaining ACID properties in distributed transaction scenarios
- Only called for transactions that were previously prepared via pgoutput_prepare_txn
- Does not need empty transaction optimization since prepared transactions must have changes