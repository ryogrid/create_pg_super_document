# ReorderBufferProcessXid

## Location
src/backend/replication/logical/reorderbuffer.c: 3170 - 3182

## Overview
ReorderBufferProcessXid registers a transaction ID (xid) with the reorder buffer when it is first encountered in the WAL stream, enabling the buffer to maintain transaction ordering for logical decoding.

## Definition
```c
void ReorderBufferProcessXid(ReorderBuffer *rb, TransactionId xid, XLogRecPtr lsn)
```

## Detailed Description
This function serves as a central registration point for transaction IDs encountered during WAL stream processing for logical decoding. The reorder buffer maintains data structures ordered by LSN for efficiency, which requires knowledge of when transactions first appear in the WAL. Since many WAL record types are not relevant for logical decoding, this function provides a centralized mechanism to track only the transactions that matter.

The function performs a simple but crucial task: it checks if the provided transaction ID is valid (not InvalidTransactionId) and, if so, calls ReorderBufferTXNByXid to create or update the transaction entry in the reorder buffer. This ensures that the reorder buffer has proper tracking structures for all relevant transactions.

## Parameters
- `rb`: Pointer to the ReorderBuffer instance that will track the transaction
- `xid`: The TransactionId encountered in the WAL record
- `lsn`: The Log Sequence Number where this transaction ID was encountered

## Dependencies
- Functions called/Symbols referenced:
  - ReorderBufferTXNByXid
- Called from (representative examples):
  - LogicalDecodingProcessRecord
  - xlog_decode
  - xact_decode
  - standby_decode
  - heap2_decode
  - heap_decode
  - logicalmsg_decode

## Notes and Other Information
- This function acts as a filter, only processing valid transaction IDs and ignoring InvalidTransactionId
- It must be called at least once for every xid in XLogRecord->xl_xid
- Many WAL record types do not pass through this function if they are not relevant to logical decoding
- The function is essential for maintaining LSN-ordered data structures in the reorder buffer for efficient transaction processing