# HeapTupleSatisfiesMVCC

## Location
src/backend/access/heap/heapam_visibility.c: 960 - 1161

## Overview
HeapTupleSatisfiesMVCC determines if a heap tuple is visible according to MVCC (Multi-Version Concurrency Control) semantics for a given snapshot, implementing PostgreSQL's standard transaction isolation by checking tuple visibility against snapshot transaction boundaries.

## Definition


## Detailed Description
This function is the core implementation of PostgreSQL's MVCC visibility checking. It determines whether a tuple should be visible to a query operating under a specific snapshot. The function implements the fundamental MVCC rule: a tuple is visible if it was inserted by a transaction that committed before the snapshot was taken and has not been deleted by a transaction that committed before the snapshot was taken.

Key design principles:
- Avoids updating hint bits for transactions still running according to the snapshot, even if they're actually committed/aborted, to reduce contention
- Uses XidInMVCCSnapshot to check if transactions are visible in the given snapshot
- Handles command-level visibility within the current transaction using curcid
- Supports frozen transaction IDs for very old committed transactions
- Optimizes performance by deferring hint bit updates to reduce shared data structure access

The function carefully handles various tuple states including locked-only tuples, multi-transaction scenarios, and special cases for the current transaction.

## Parameters / Member Variables
- : The heap tuple to check for visibility, containing tuple data and metadata
- : The MVCC snapshot defining which transactions are visible, including xmin/xmax bounds and current command ID
- : The buffer containing the tuple, used for setting hint bits when appropriate for performance optimization

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderXminCommitted
  - HeapTupleHeaderXminInvalid
  - HeapTupleHeaderXminFrozen
  - HeapTupleHeaderGetXvac
  - HeapTupleHeaderGetRawXmin
  - HeapTupleHeaderGetRawXmax
  - HeapTupleHeaderGetCmin
  - HeapTupleHeaderGetCmax
  - [HeapTupleGetUpdateXid](HeapTupleGetUpdateXid.md)
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md)
  - [TransactionIdDidCommit](../T/TransactionIdDidCommit.md)
  - [XidInMVCCSnapshot](../X/XidInMVCCSnapshot.md)
  - [SetHintBits](../S/SetHintBits.md)
- Called from (representative examples):
  - [HeapTupleSatisfiesVisibility](HeapTupleSatisfiesVisibility.md)

## Notes and Other Information
The function is static and represents the standard MVCC visibility semantics used by most PostgreSQL queries. It implements careful optimization strategies to minimize contention on shared data structures like ProcArrayLock by avoiding premature hint bit updates.

The function handles legacy HEAP_MOVED_OFF and HEAP_MOVED_IN cases for pre-9.0 binary upgrade compatibility. For current transactions, it uses command ID comparison to implement statement-level read consistency within a transaction.

A critical optimization is that hint bits are only updated when transactions are definitively known to be committed/aborted according to the snapshot, avoiding the overhead of checking actual transaction status when it wouldn't change the visibility result. This design choice prioritizes reducing lock contention over immediate hint bit accuracy.