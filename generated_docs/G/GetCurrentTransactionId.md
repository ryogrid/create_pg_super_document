# GetCurrentTransactionId

## Location
src/backend/access/transam/xact.c: 451 - 467

## Overview
Returns the transaction ID (XID) of the current transaction (main or subtransaction), assigning one if it has not yet been set.

## Definition
TransactionId GetCurrentTransactionId(void)

## Detailed Description
GetCurrentTransactionId is a transaction management function that retrieves the transaction ID of the current transaction context, which may be either a main transaction or a subtransaction (savepoint). Unlike GetTopTransactionId which always returns the main transaction's XID, this function returns the XID of whichever transaction level is currently active.

The function operates on the current transaction state and performs lazy assignment of transaction IDs. If the current transaction context does not yet have an XID assigned, the function will automatically assign one by calling AssignTransactionId on the current transaction state.

Key characteristics:
1. Works with both main transactions and subtransactions
2. Performs lazy XID assignment when needed
3. Returns the XID appropriate for the current transaction nesting level
4. Should only be called within a valid transaction context

This function is crucial for operations that need to identify the specific transaction level that should be recorded for visibility, locking, or logging purposes.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - TransactionState (current transaction state type)
  - FullTransactionIdIsValid (checks if transaction ID is valid)
  - AssignTransactionId (assigns a new transaction ID)
  - XidFromFullTransactionId (converts full XID to regular XID)
- Called from (representative examples):
  - heap_insert
  - heap_multi_insert
  - heap_delete
  - heap_update
  - heap_lock_tuple
  - heap_abort_speculative
  - PrepareTransaction
  - ExecInsert
  - LogLogicalMessage
  - RelationSetNewRelfilenumber

## Notes and Other Information
- This function is essential for heap operations and tuple visibility management
- Used extensively in data modification operations (INSERT, UPDATE, DELETE) where the modifying transaction must be recorded
- Critical for subtransaction handling and savepoint functionality
- Different from GetTopTransactionId in that it respects the current transaction nesting level
- Widely used in executor nodes and heap access methods for proper transaction identification
- Important for logical replication where the correct transaction context must be preserved
- Essential for lock management and prepared transactions where transaction identity is crucial for cleanup and recovery