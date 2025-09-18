# ReorderBufferReplay

## Location
src/backend/replication/logical/reorderbuffer.c: 2716 - 2776

## Overview
Main entry point for replaying a completed transaction and its non-aborted subtransactions in logical replication, handling both streamed and non-streamed transaction scenarios.

## Definition
```c
static void ReorderBufferReplay(ReorderBufferTXN *txn, ReorderBuffer *rb, TransactionId xid,
                               XLogRecPtr commit_lsn, XLogRecPtr end_lsn, TimestampTz commit_time,
                               RepOriginId origin_id, XLogRecPtr origin_lsn)
```

## Detailed Description
ReorderBufferReplay is the primary orchestrator for transaction replay in PostgreSQL's logical replication system. It is invoked once a prepare or toplevel commit record is encountered for both streamed and non-streamed transactions. The function first populates transaction metadata (LSNs, commit time, origin information), then determines the appropriate replay strategy. For streamed transactions, it delegates to ReorderBufferStreamCommit for specialized streaming replay. For non-streamed transactions, it validates that the transaction has changes to replay (base_snapshot exists) and then calls ReorderBufferProcessTXN to perform the actual change processing and output.

## Parameters / Member Variables
- `txn`: Transaction to replay, including all its committed subtransactions
- `rb`: ReorderBuffer instance managing replication state and output callbacks
- `xid`: Transaction ID being replayed
- `commit_lsn`: LSN of the commit record
- `end_lsn`: End LSN of the transaction in the WAL
- `commit_time`: Timestamp when the transaction was committed
- `origin_id`: Replication origin ID for this transaction
- `origin_lsn`: LSN at the origin for this transaction

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBuffer](ReorderBuffer.md) (struct type)
  - [ReorderBufferTXN](ReorderBufferTXN.md) (struct type)
  - RepOriginId (type)
  - CommandId/FirstCommandId (command ID management)
  - rbtxn_is_streamed (check if transaction was streamed)
  - [ReorderBufferStreamCommit](ReorderBufferStreamCommit.md) (streaming commit handling)
  - rbtxn_prepared (check if transaction is prepared)
  - [ReorderBufferCleanupTXN](ReorderBufferCleanupTXN.md) (cleanup transaction resources)
  - [ReorderBufferProcessTXN](ReorderBufferProcessTXN.md) (core transaction processing)
- Called from (representative examples):
  - [ReorderBufferCommit](ReorderBufferCommit.md) (regular transaction commit)
  - [ReorderBufferPrepare](ReorderBufferPrepare.md) (prepared transaction handling)
  - [ReorderBufferFinishPrepared](ReorderBufferFinishPrepared.md) (finishing prepared transactions)

## Notes and Other Information
- This function serves as the main entry point for transaction replay in logical replication
- Handles the distinction between streamed and non-streamed transaction replay paths
- Validates that transactions have actual changes before attempting replay (base_snapshot check)
- Properly sets up transaction metadata before delegating to specialized processing functions
- Includes optimization to avoid processing transactions with no database changes
- Essential for both regular commits and prepared transaction scenarios
- Ensures proper cleanup of empty transactions to maintain restart_lsn computation accuracy
- Part of the high-level transaction lifecycle management in PostgreSQL's logical replication system