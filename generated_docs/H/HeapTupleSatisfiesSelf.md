# HeapTupleSatisfiesSelf

## Location
src/backend/access/heap/heapam_visibility.c: 170 - 339

## Overview
Determines if a heap tuple is visible to the current transaction under the "self" visibility rule, which means the tuple is visible if it was created by the current transaction and not deleted by it.

## Definition


## Detailed Description
HeapTupleSatisfiesSelf implements the "self" visibility semantics for PostgreSQL's MVCC system. This function determines whether a tuple should be visible to the current transaction by examining the transaction IDs in the tuple header and comparing them with the current transaction.

The visibility logic follows these key principles:
- A tuple is visible if it was inserted by the current transaction (Xmin matches current txn)
- A tuple is not visible if it was deleted by the current transaction (Xmax matches current txn)
- A tuple is visible if it was committed by another transaction and not deleted by a committed transaction
- Special handling exists for pre-9.0 VACUUM FULL operations (HEAP_MOVED_OFF/HEAP_MOVED_IN flags)
- Multixact handling for cases where multiple transactions have locked/updated the tuple

The function also performs hint bit optimization by calling SetHintBits to cache transaction commit/abort status for future visibility checks.

## Parameters / Member Variables
- `htup`: The heap tuple to check for visibility
- `snapshot`: Snapshot context (not used in self-visibility but required for interface consistency)
- `buffer`: Buffer containing the tuple, used for hint bit updates

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderXminCommitted
  - HeapTupleHeaderXminInvalid
  - HeapTupleHeaderGetXvac
  - HeapTupleHeaderGetRawXmin
  - HeapTupleHeaderGetRawXmax
  - [HeapTupleGetUpdateXid](HeapTupleGetUpdateXid.md)
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md)
  - TransactionIdIsInProgress
  - [TransactionIdDidCommit](../T/TransactionIdDidCommit.md)
  - [SetHintBits](../S/SetHintBits.md)
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md)
- Called from (representative examples):
  - [HeapTupleSatisfiesVisibility](HeapTupleSatisfiesVisibility.md)

## Notes and Other Information
- This function is part of PostgreSQL's pluggable tuple visibility system
- Used primarily for READ COMMITTED isolation level and similar contexts
- The function handles complex multixact scenarios where tuples may be locked by multiple transactions
- Legacy support exists for pre-9.0 database upgrades via HEAP_MOVED_OFF/HEAP_MOVED_IN handling
- Performance is optimized through aggressive hint bit setting to avoid repeated transaction status lookups
- The function assumes the input tuple is valid and performs assertion checks on tuple consistency