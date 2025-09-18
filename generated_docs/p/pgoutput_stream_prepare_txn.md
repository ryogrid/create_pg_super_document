# pgoutput_stream_prepare_txn

## Location
src/backend/replication/pgoutput/pgoutput.c: 1897 - 1916

## Overview
This function handles the PREPARE callback for streaming two-phase commit transactions, notifying downstream subscribers to prepare a streamed transaction for commit.

## Definition
```c
static void
pgoutput_stream_prepare_txn(LogicalDecodingContext *ctx,
                            ReorderBufferTXN *txn,
                            XLogRecPtr prepare_lsn)
```

## Detailed Description
The `pgoutput_stream_prepare_txn` function is part of PostgreSQL's logical replication system, specifically handling the prepare phase of two-phase commit (2PC) transactions that have been streamed. This function is called when a streamed transaction reaches the prepare phase in a distributed transaction scenario. It sends a stream prepare message to downstream subscribers, allowing them to prepare the transaction for eventual commit or abort. The function ensures the transaction is properly marked as streamed before proceeding.

## Parameters / Member Variables
- `ctx`: LogicalDecodingContext pointer containing the decoding context and output plugin state
- `txn`: ReorderBufferTXN pointer representing the transaction being prepared
- `prepare_lsn`: XLogRecPtr specifying the WAL location where the transaction was prepared

## Dependencies
- Functions called/Symbols referenced:
  - rbtxn_is_streamed
  - [OutputPluginUpdateProgress](../O/OutputPluginUpdateProgress.md)
  - [OutputPluginPrepareWrite](../O/OutputPluginPrepareWrite.md)
  - [logicalrep_write_stream_prepare](../l/logicalrep_write_stream_prepare.md)
  - [OutputPluginWrite](../O/OutputPluginWrite.md)
- Called from (representative examples):
  - [_PG_output_plugin_init](../P/_PG_output_plugin_init.md) (registered as callback)

## Notes and Other Information
- This function is specifically for streaming two-phase commit transactions
- It includes an assertion to verify that the transaction is marked as streamed (rbtxn_is_streamed(txn))
- The function is registered as a callback during plugin initialization in _PG_output_plugin_init
- Unlike the commit callback, this function does not perform relation cache cleanup since the transaction is only being prepared, not committed
- This is part of PostgreSQL's support for distributed transactions where prepare and commit are separate phases
- The prepare_lsn parameter allows downstream systems to track the exact WAL position of the prepare operation