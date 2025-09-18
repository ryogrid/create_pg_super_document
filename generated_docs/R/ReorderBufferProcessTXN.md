# ReorderBufferProcessTXN

## Location
src/backend/replication/logical/reorderbuffer.c: 2127 - 2514

## Overview
Core helper function that processes and replays all changes in a transaction (and its subtransactions) in LSN order, supporting both regular replay and streaming modes for logical replication.

## Definition
```c
static void ReorderBufferProcessTXN(ReorderBuffer *rb, ReorderBufferTXN *txn,
                                   XLogRecPtr commit_lsn, volatile Snapshot snapshot_now,
                                   volatile CommandId command_id, bool streaming)
```

## Detailed Description
ReorderBufferProcessTXN is the central engine for transaction processing in PostgreSQL's logical replication system. It performs a k-way merge of changes from the main transaction and all subtransactions, processing them in LSN order to maintain consistency. The function handles multiple types of changes including INSERT/UPDATE/DELETE operations, truncates, messages, invalidations, snapshots, and command ID updates. It supports both streaming and non-streaming modes, manages toast data reconstruction, handles speculative insertions, and maintains proper transaction isolation through snapshot management. The function also includes comprehensive error handling and resource cleanup mechanisms.

## Parameters / Member Variables
- `rb`: ReorderBuffer instance managing the replication state and callbacks
- `txn`: Main transaction to process along with all its subtransactions
- `commit_lsn`: LSN of the commit record for this transaction
- `snapshot_now`: Current snapshot for visibility determination (marked volatile for PG_TRY)
- `command_id`: Current command ID for proper tuple visibility (marked volatile for PG_TRY)
- `streaming`: Boolean flag indicating whether to use streaming API instead of regular replay

## Dependencies
- Functions called/Symbols referenced:
  - ReorderBufferBuildTupleCidHash (build tuple command ID hash)
  - SetupHistoricSnapshot (setup snapshot for decoding)
  - ReorderBufferIterTXNInit/ReorderBufferIterTXNNext (transaction iteration)
  - ReorderBufferApplyChange (apply individual changes)
  - ReorderBufferApplyMessage (apply messages)
  - ReorderBufferApplyTruncate (apply truncate operations)
  - ReorderBufferToastReplace/ReorderBufferToastReset (toast handling)
  - Various relation and snapshot management functions
- Called from (representative examples):
  - ReorderBufferReplay (regular transaction replay)
  - ReorderBufferStreamTXN (streaming transaction processing)

## Notes and Other Information
- This is the core processing engine for logical replication in PostgreSQL
- Implements k-way merge algorithm to process changes from multiple subtransactions in proper order
- Handles complex scenarios like speculative insertions, toast data reconstruction, and catalog changes
- Uses PostgreSQL's internal transaction system for proper resource management and error handling
- Supports both streaming and batch processing modes for different replication scenarios
- Includes extensive error checking and validation to ensure data consistency
- The function is marked with volatile parameters due to PG_TRY exception handling requirements
- Critical for maintaining transactional consistency and proper ordering in logical replication streams