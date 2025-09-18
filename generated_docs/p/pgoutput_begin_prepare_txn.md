# pgoutput_begin_prepare_txn

## Location
src/backend/replication/pgoutput/pgoutput.c: 642 - 658

## Overview
Sends a BEGIN PREPARE message for prepared transactions in logical replication, initiating the prepare phase of two-phase commit.

## Definition
static void pgoutput_begin_prepare_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)

## Detailed Description
This function is the begin prepare callback for the pgoutput logical replication plugin, used in two-phase commit scenarios. It sends a BEGIN PREPARE message to subscribers, indicating the start of the prepare phase for a transaction. This is part of the two-phase commit protocol support in logical replication, where transactions can be prepared on the publisher and then either committed or aborted later. The function handles replication origin information for cases where the transaction originated from another node in a replication topology.

## Parameters / Member Variables
- ctx: LogicalDecodingContext pointer containing the output stream and plugin context
- txn: ReorderBufferTXN pointer representing the transaction being prepared

## Dependencies
- Functions called/Symbols referenced:
  - OutputPluginPrepareWrite
  - logicalrep_write_begin_prepare
  - send_repl_origin
  - OutputPluginWrite
  - InvalidRepOriginId (constant)
- Called from (representative examples):
  - _PG_output_plugin_init (registered as begin prepare callback)

## Notes and Other Information
- This is a static function internal to the pgoutput plugin
- Part of PostgreSQL two-phase commit support for logical replication
- Conditionally includes replication origin information based on transaction origin ID
- Works in conjunction with pgoutput_prepare_txn and pgoutput_commit_prepared_txn
- Simpler than regular begin callback as it doesn't track transaction state like sent_begin_txn flag
- Essential for maintaining consistency in distributed transaction scenarios