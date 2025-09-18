# ReorderBufferPrepare

## Location
src/backend/replication/logical/reorderbuffer.c: 2846 - 2882

## Overview
Processes a two-phase transaction prepare operation by setting prepare flags and replaying the transaction changes.

## Definition
```c
void ReorderBufferPrepare(ReorderBuffer *rb, TransactionId xid, char *gid)
```

## Detailed Description
ReorderBufferPrepare handles the prepare phase of a two-phase commit transaction in logical replication. It marks the transaction with the RBTXN_PREPARE flag, stores the global transaction identifier (GID), and calls ReorderBufferReplay to process the transaction changes. The function expects that prepare information has already been recorded in the transaction structure via ReorderBufferRememberPrepareInfo. For concurrently aborted transactions that are not streamed, it also sends a prepare message to ensure downstream systems can properly handle subsequent rollback prepared operations.

## Parameters / Member Variables
- `rb`: The ReorderBuffer instance managing transactions
- `xid`: Transaction ID of the transaction to prepare
- `gid`: Global transaction identifier for the two-phase transaction

## Dependencies
- Functions called/Symbols referenced:
  - [ReorderBufferTXNByXid](ReorderBufferTXNByXid.md)
  - RBTXN_PREPARE (flag constant)
  - [ReorderBufferReplay](ReorderBufferReplay.md)
  - rbtxn_is_streamed
- Called from (representative examples):
  - [DecodePrepare](../D/DecodePrepare.md)

## Notes and Other Information
This function is a key component of two-phase commit support in PostgreSQL logical replication. It relies on prepare information being previously stored by ReorderBufferRememberPrepareInfo and uses pstrdup to create a persistent copy of the GID. The special handling for concurrent_abort cases ensures that downstream subscribers receive prepare messages even for transactions that will ultimately be rolled back, enabling proper cleanup of prepared transactions.