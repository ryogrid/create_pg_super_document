# AtSubCommit_childXids

## Location
src/backend/access/transam/xact.c: 1633 - 1722

## Overview
AtSubCommit_childXids is a static function responsible for passing the current subtransaction's XID and all its child XIDs up to its parent transaction as committed children during subtransaction commit processing.

## Definition


## Detailed Description
This function handles the critical task of propagating transaction identifiers from a committing subtransaction to its parent transaction. When a subtransaction commits, it must transfer its own XID and all XIDs of its committed child subtransactions to the parent's childXids array. This maintains the hierarchical relationship and ensures that the parent transaction tracks all committed descendants.

The function dynamically manages memory allocation for the parent's childXids array, expanding it as needed to accommodate the additional XIDs. It employs a growth strategy that doubles the required size (up to MaxAllocSize) to minimize frequent reallocations. The XIDs are copied in a specific order that preserves the chronological ordering constraint - child XIDs always follow their parent XIDs.

## Parameters / Member Variables
This function takes no parameters and operates on the global CurrentTransactionState.

## Dependencies
- Functions called/Symbols referenced:
  - TransactionState (current transaction state access)
  - MaxAllocSize (memory allocation limit)
  - MemoryContextAlloc (memory allocation for new arrays)
  - repalloc (memory reallocation for existing arrays)
  - XidFromFullTransactionId (XID extraction from full transaction ID)
- Called from:
  - CommitSubTransaction (during subtransaction commit at src/backend/access/transam/xact.c:5089)

## Notes and Other Information
- The function maintains XID ordering by design - child XIDs always follow parent XIDs chronologically
- Memory allocation uses TopTransactionContext to avoid creating child-transaction contexts for potentially small amounts of grandchild XID data
- The function includes protection against integer overflow and enforces PostgreSQL's limit on committed subtransactions
- After successful copying, the function cleans up the child's XID array to prevent memory leaks and double-free scenarios
- This is a critical component of PostgreSQL's nested transaction commit protocol