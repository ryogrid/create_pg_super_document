# GetTopTransactionId

## Location
src/backend/access/transam/xact.c: 423 - 437

## Overview
Returns the transaction ID (XID) of the main transaction, assigning one if it has not yet been set.

## Definition
TransactionId GetTopTransactionId(void)

## Detailed Description
GetTopTransactionId is a transaction management function that retrieves the transaction ID of the top-level (main) transaction. If the transaction ID has not been assigned yet, the function will automatically assign one by calling AssignTransactionId.

This function is critical for transaction identification and logging purposes. It ensures that every transaction that needs an XID gets one, but only when actually required (lazy assignment). The function operates on the global XactTopFullTransactionId which represents the full transaction ID of the current top-level transaction.

The function performs the following steps:
1. Checks if XactTopFullTransactionId is valid using FullTransactionIdIsValid
2. If not valid, assigns a new transaction ID by calling AssignTransactionId on TopTransactionStateData
3. Converts the full transaction ID to a regular TransactionId using XidFromFullTransactionId and returns it

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionIdIsValid (checks if transaction ID is valid)
  - AssignTransactionId (assigns a new transaction ID)
  - XidFromFullTransactionId (converts full XID to regular XID)
- Called from (representative examples):
  - log_heap_new_cid
  - AssignTransactionId
  - fill_seq_fork_with_data
  - AlterSequence
  - SequenceChangePersistence
  - nextval_internal
  - do_setval

## Notes and Other Information
- This function should only be called within a valid transaction context
- Uses lazy assignment - transaction IDs are only assigned when actually needed
- Critical for sequence operations and heap operations that require transaction identification
- The returned TransactionId is used for visibility checks, WAL logging, and transaction isolation
- Works with the global transaction state and is part of PostgreSQL's transaction ID management system
- Used extensively in sequence manipulation functions where transaction identity is important for concurrency control