# xactGetCommittedChildren

## Location
src/backend/access/transam/xact.c: 5728 - 5751

## Overview
A function that retrieves the list of committed child transaction IDs for the current transaction in PostgreSQL's transaction management system.

## Definition


## Detailed Description
xactGetCommittedChildren provides access to the committed child transactions of the current transaction. This function is essential for transaction management operations that need to track sub-transaction relationships, particularly for WAL logging, two-phase commit, and snapshot management. The function returns both the count of child transactions and sets a pointer to the array of child transaction IDs. The memory for the child transaction array is managed in TopTransactionContext and should not be freed by the caller. This design ensures that the child transaction information remains available throughout the transaction's lifetime and avoids memory management issues.

## Parameters / Member Variables
- : A pointer to a TransactionId pointer that will be set to point to the array of child transaction IDs, or NULL if no child transactions exist

## Dependencies
- Functions called/Symbols referenced:
  - TransactionState (struct type)
  - CurrentTransactionState (global variable)
- Called from (representative examples):
  - [StartPrepare](../S/StartPrepare.md) (in two-phase commit)
  - [RecordTransactionCommit](../R/RecordTransactionCommit.md) (for WAL logging)
  - [RecordTransactionAbort](../R/RecordTransactionAbort.md) (for abort logging)
  - [ExportSnapshot](../E/ExportSnapshot.md) (for snapshot management)

## Notes and Other Information
- Returns the number of committed child transactions as an integer
- Sets *ptr to NULL when there are no child transactions (nChildXids == 0)
- The returned array is allocated in TopTransactionContext and must NOT be freed by the caller (important change from pre-8.4 behavior)
- Critical for maintaining transactional integrity across sub-transaction hierarchies
- Used extensively in WAL logging to ensure all child transactions are properly recorded
- Essential for two-phase commit protocol to track all participating sub-transactions
- The child XIDs array contains only committed child transactions, not aborted ones