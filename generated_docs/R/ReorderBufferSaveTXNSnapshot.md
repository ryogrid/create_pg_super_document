# ReorderBufferSaveTXNSnapshot

## Location
src/backend/replication/logical/reorderbuffer.c: 2060 - 2080

## Overview
Stores the current command ID and snapshot state in a transaction for reuse during streaming logical replication, optimizing snapshot management across stream boundaries.

## Definition
```c
static inline void ReorderBufferSaveTXNSnapshot(ReorderBuffer *rb, ReorderBufferTXN *txn,
                                               Snapshot snapshot_now, CommandId command_id)
```

## Detailed Description
ReorderBufferSaveTXNSnapshot is responsible for preserving transaction state (command ID and snapshot) at the end of a streaming operation so it can be efficiently reused when the next stream begins. This function implements an optimization to avoid unnecessary snapshot copying - if the snapshot is already copied, it reuses it directly; otherwise, it creates a new copy using ReorderBufferCopySnap. This is crucial for maintaining consistent visibility and transaction isolation across streaming boundaries in logical replication.

## Parameters / Member Variables
- `rb`: ReorderBuffer instance managing the replication state
- `txn`: Transaction to save the snapshot state for
- `snapshot_now`: Current snapshot to be saved
- `command_id`: Current command ID to be saved

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBuffer](ReorderBuffer.md) (struct type)
  - [ReorderBufferTXN](ReorderBufferTXN.md) (struct type)
  - CommandId (type)
  - [ReorderBufferCopySnap](ReorderBufferCopySnap.md) (function for snapshot copying)
- Called from (representative examples):
  - [ReorderBufferResetTXN](ReorderBufferResetTXN.md)
  - CHANGES_THRESHOLD (related functionality)

## Notes and Other Information
- This is a static inline function optimized for frequent calls during streaming operations
- Implements copy-on-write optimization for snapshots to avoid unnecessary duplication
- Essential for maintaining transaction consistency across streaming boundaries in logical replication
- The saved state allows seamless continuation of transaction processing in subsequent streams
- Part of PostgreSQL's streaming logical replication optimization infrastructure