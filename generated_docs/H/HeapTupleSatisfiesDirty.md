# HeapTupleSatisfiesDirty

## Location
[src/backend/access/heap/heapam_visibility.c:743-959](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_visibility.c#L743-L959)

## Overview
HeapTupleSatisfiesDirty determines if a heap tuple is visible including effects of open (in-progress) transactions, implementing PostgreSQL's SNAPSHOT_DIRTY visibility semantics by returning transaction IDs of concurrent transactions affecting the tuple.

## Definition


## Detailed Description
This function implements the "dirty read" visibility semantics for PostgreSQL's SNAPSHOT_DIRTY snapshots. Unlike other visibility functions that only consider committed transactions, HeapTupleSatisfiesDirty includes the effects of transactions still in progress. This is essential for certain internal operations that need to see uncommitted changes.

The function serves dual purposes:
1. Returns a boolean indicating tuple visibility under dirty read semantics
2. Uses the snapshot parameter as an output mechanism to report concurrent transaction IDs

Key behaviors include:
- Setting snapshot->xmin to the inserting transaction ID if it's still in progress
- Setting snapshot->xmax to the updating/deleting transaction ID if it's still in progress  
- Handling speculative insertions by returning the speculative token
- Similar to HeapTupleSatisfiesSelf for current transaction and committed transactions
- Includes effects of other in-progress transactions unlike standard visibility checks

## Parameters / Member Variables
- : The heap tuple to check for visibility, containing tuple data and metadata
- : Input/output parameter used to return concurrent transaction IDs affecting the tuple (xmin, xmax, speculativeToken)
- : The buffer containing the tuple, used for setting hint bits to optimize future visibility checks

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderXminCommitted
  - HeapTupleHeaderXminInvalid
  - HeapTupleHeaderGetXvac
  - HeapTupleHeaderGetRawXmin
  - HeapTupleHeaderGetRawXmax
  - HeapTupleHeaderIsSpeculative
  - HeapTupleHeaderGetSpeculativeToken
  - [HeapTupleGetUpdateXid](HeapTupleGetUpdateXid.md)
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md)
  - TransactionIdIsInProgress
  - [TransactionIdDidCommit](../T/TransactionIdDidCommit.md)
  - [SetHintBits](../S/SetHintBits.md)
- Called from (representative examples):
  - [HeapTupleSatisfiesVisibility](HeapTupleSatisfiesVisibility.md)

## Notes and Other Information
The function is static and primarily used internally by the visibility subsystem. It implements SNAPSHOT_DIRTY semantics which are used for specific internal operations that need to observe uncommitted changes from other transactions.

The snapshot parameter serves as both input and output - the function modifies snapshot->xmin, snapshot->xmax, and snapshot->speculativeToken to inform the caller about concurrent transactions affecting the tuple. This information is crucial for operations that need to track or wait for concurrent transactions.

Special handling is included for speculative insertions, where the inserting transaction might still back down without aborting the entire transaction. The speculative token is returned to allow proper coordination between concurrent operations.