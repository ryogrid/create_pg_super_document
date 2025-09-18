# pgoutput_prepare_txn

## Location
[src/backend/replication/pgoutput/pgoutput.c:659-672](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L659-L672)

## Overview
Handles the PREPARE callback for two-phase commit transactions in logical replication, sending prepare messages to subscribers.

## Definition
static void pgoutput_prepare_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr prepare_lsn)

## Detailed Description
This function is the prepare callback for the pgoutput logical replication plugin, implementing the prepare phase of two-phase commit protocol. It is called when a transaction is prepared (but not yet committed or aborted) and sends a PREPARE message to subscribers. This allows subscribers to prepare the transaction on their side, maintaining consistency in distributed transaction scenarios. The function updates replication progress and writes the prepare information to the logical replication stream.

## Parameters / Member Variables
- ctx: LogicalDecodingContext pointer containing the output stream and plugin context
- txn: ReorderBufferTXN pointer representing the transaction being prepared
- prepare_lsn: XLogRecPtr indicating the LSN where the transaction was prepared

## Dependencies
- Functions called/Symbols referenced:
  - [OutputPluginUpdateProgress](../O/OutputPluginUpdateProgress.md)
  - [OutputPluginPrepareWrite](../O/OutputPluginPrepareWrite.md)
  - [logicalrep_write_prepare](../l/logicalrep_write_prepare.md)
  - [OutputPluginWrite](../O/OutputPluginWrite.md)
- Called from (representative examples):
  - [_PG_output_plugin_init](../P/_PG_output_plugin_init.md) (registered as prepare callback)

## Notes and Other Information
- This is a static function internal to the pgoutput plugin
- Part of PostgreSQL two-phase commit support for logical replication
- Always updates progress (second parameter to OutputPluginUpdateProgress is false)
- Simpler than commit callback as it doesn't need to handle empty transaction optimization
- Works in conjunction with pgoutput_begin_prepare_txn and pgoutput_commit_prepared_txn
- Essential for maintaining ACID properties in distributed transaction scenarios
- The prepared transaction can later be committed via pgoutput_commit_prepared_txn or aborted