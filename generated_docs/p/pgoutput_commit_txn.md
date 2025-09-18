# pgoutput_commit_txn

## Location
src/backend/replication/pgoutput/pgoutput.c: 610 - 641

## Overview
Handles the COMMIT callback for logical replication transactions, sending commit messages to subscribers when the transaction contains relevant changes.

## Definition
static void pgoutput_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr commit_lsn)

## Detailed Description
This function is the commit callback for the pgoutput logical replication plugin. It is responsible for finalizing a transaction in the logical replication stream by sending a COMMIT message to subscribers. The function includes an optimization to skip empty transactions (those without relevant changes) to avoid unnecessary network traffic. It also properly cleans up transaction-specific data and updates the replication progress.

## Parameters / Member Variables
- ctx: LogicalDecodingContext pointer containing the output stream and plugin context
- txn: ReorderBufferTXN pointer representing the transaction being committed
- commit_lsn: XLogRecPtr indicating the LSN where the transaction was committed

## Dependencies
- Functions called/Symbols referenced:
  - [OutputPluginUpdateProgress](../O/OutputPluginUpdateProgress.md)
  - [pfree](pfree.md)
  - elog
  - [OutputPluginPrepareWrite](../O/OutputPluginPrepareWrite.md)
  - [logicalrep_write_commit](../l/logicalrep_write_commit.md)
  - [OutputPluginWrite](../O/OutputPluginWrite.md)
  - [PGOutputTxnData](../P/PGOutputTxnData.md) (struct type)
  - DEBUG1 (logging level)
- Called from (representative examples):
  - [_PG_output_plugin_init](../P/_PG_output_plugin_init.md) (registered as commit callback)

## Notes and Other Information
- This is a static function internal to the pgoutput plugin
- Implements an optimization to skip replication of empty transactions
- Properly cleans up txndata memory using pfree()
- Sets txn->output_plugin_private to NULL after cleanup
- Logs debug information when skipping empty transactions
- Only sends commit messages for transactions that had a BEGIN message sent
- Updates replication progress regardless of whether the transaction is replicated