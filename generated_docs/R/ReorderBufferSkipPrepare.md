# ReorderBufferSkipPrepare

## Location
src/backend/replication/logical/reorderbuffer.c: 2827 - 2845

## Overview
Marks a transaction with a flag indicating that its prepare phase has been skipped during logical replication processing.

## Definition
```c
void ReorderBufferSkipPrepare(ReorderBuffer *rb, TransactionId xid)
```

## Detailed Description
ReorderBufferSkipPrepare sets the RBTXN_SKIPPED_PREPARE flag on a transaction to indicate that the prepare phase was not processed during logical decoding. This occurs when prepare records are encountered but not decoded due to various conditions such as lacking a consistent snapshot or two-phase commit being disabled. The flag helps track the transaction state for later processing during commit prepared, ensuring proper handling of transactions that bypassed the normal prepare workflow.

## Parameters / Member Variables
- `rb`: The ReorderBuffer instance managing transactions
- `xid`: Transaction ID of the transaction whose prepare was skipped

## Dependencies
- Functions called/Symbols referenced:
  - ReorderBufferTXNByXid
  - RBTXN_SKIPPED_PREPARE (flag constant)
- Called from (representative examples):
  - DecodePrepare

## Notes and Other Information
This function is called from DecodePrepare when certain conditions prevent normal prepare processing. The RBTXN_SKIPPED_PREPARE flag is later checked during commit prepared processing to determine the appropriate handling. This mechanism ensures that transactions maintain proper state tracking even when their prepare phase cannot be immediately processed during logical replication.