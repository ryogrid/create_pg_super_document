# SetHintBits

## Location
src/backend/access/heap/heapam_visibility.c: 114 - 140

## Overview
Sets commit/abort hint bits on a tuple header when it is safe to do so, optimizing future visibility checks by caching transaction status information directly in the tuple header.

## Definition


## Detailed Description
SetHintBits is a critical optimization function that sets hint bits in tuple headers to cache the commit/abort status of transactions. The function ensures that hint bits are only set when it is safe to do so, considering durability constraints and WAL flushing requirements.

The function implements sophisticated logic to ensure data consistency:
- For committed transactions, it only sets hint bits if the transaction's commit record is guaranteed to be flushed to disk before the buffer, or if the table is temporary/unlogged
- For aborted transactions, hint bits can always be set safely
- It uses LSN (Log Sequence Number) comparison to ensure proper ordering between WAL records and data pages
- For permanent tables, it checks if the commit LSN has been flushed and compares buffer LSN with commit LSN

## Parameters / Member Variables
- `tuple`: Pointer to the heap tuple header where hint bits will be set
- `buffer`: Buffer containing the tuple, used for durability checks and marking dirty
- `infomask`: Bitmask specifying which hint bits to set (e.g., HEAP_XMIN_COMMITTED, HEAP_XMAX_COMMITTED)
- `xid`: Transaction ID to check for commit status, or InvalidTransactionId if no check needed

## Dependencies
- Functions called/Symbols referenced:
  - [TransactionIdGetCommitLSN](../T/TransactionIdGetCommitLSN.md)
  - [XLogNeedsFlush](../X/XLogNeedsFlush.md)
  - [BufferIsPermanent](../B/BufferIsPermanent.md)
  - [BufferGetLSNAtomic](../B/BufferGetLSNAtomic.md)
  - [MarkBufferDirtyHint](../M/MarkBufferDirtyHint.md)
- Called from (representative examples):
  - [HeapTupleSetHintBits](../H/HeapTupleSetHintBits.md)
  - [HeapTupleSatisfiesSelf](../H/HeapTupleSatisfiesSelf.md)
  - [HeapTupleSatisfiesToast](../H/HeapTupleSatisfiesToast.md)
  - [HeapTupleSatisfiesUpdate](../H/HeapTupleSatisfiesUpdate.md)
  - [HeapTupleSatisfiesDirty](../H/HeapTupleSatisfiesDirty.md)
  - [HeapTupleSatisfiesMVCC](../H/HeapTupleSatisfiesMVCC.md)
  - [HeapTupleSatisfiesVacuumHorizon](../H/HeapTupleSatisfiesVacuumHorizon.md)

## Notes and Other Information
- This is a static inline function, providing performance optimization for frequently called visibility checks
- The function is essential for PostgreSQL's MVCC implementation, reducing the need for repeated transaction status lookups
- Special handling exists for HEAP_MOVED_IN/HEAP_MOVED_OFF entries from pre-9.0 VACUUM FULL operations
- The LSN interlock mechanism prevents race conditions between WAL flushing and hint bit setting
- Hint bits are a performance optimization and their absence does not affect correctness, only performance