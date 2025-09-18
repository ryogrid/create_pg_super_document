# RestoreTransactionCharacteristics

## Location
src/backend/access/transam/xact.c: 3080 - 3092

## Overview
RestoreTransactionCharacteristics restores previously saved transaction characteristics (isolation level, read-only status, deferrable property) to enable transaction chaining with consistent properties.

## Definition
```c
void RestoreTransactionCharacteristics(const SavedTransactionCharacteristics *s)
```

## Detailed Description
RestoreTransactionCharacteristics is the counterpart function to SaveTransactionCharacteristics, designed to restore transaction characteristics that were previously saved. This function is essential for transaction chaining functionality in PostgreSQL, where a new transaction needs to inherit the same characteristics as the previous one.

The function performs the reverse operation of SaveTransactionCharacteristics by copying the saved values from the provided structure back to the global transaction state variables. This ensures that the current transaction adopts the same isolation level, read-only mode, and deferrable status that were active in a previous transaction.

This restoration is necessary because PostgreSQL's GUC system automatically resets transaction characteristics at the end of each transaction, so manual restoration is required to maintain continuity in transaction chaining scenarios.

## Parameters / Member Variables
- `s`: Pointer to const SavedTransactionCharacteristics structure containing the saved transaction characteristics to restore
  - `save_XactIsoLevel`: The saved transaction isolation level to restore to XactIsoLevel
  - `save_XactReadOnly`: The saved read-only transaction status to restore to XactReadOnly
  - `save_XactDeferrable`: The saved deferrable transaction status to restore to XactDeferrable

## Dependencies
- Functions called/Symbols referenced:
  - SavedTransactionCharacteristics (structure)
  - XactIsoLevel (global variable - target)
  - XactReadOnly (global variable - target)
  - XactDeferrable (global variable - target)
- Called from (representative examples):
  - [CommitTransactionCommandInternal](../C/CommitTransactionCommandInternal.md) (multiple locations)
  - _SPI_commit
  - [_SPI_rollback](../S/_SPI_rollback.md)

## Notes and Other Information
- This function is part of PostgreSQL's transaction chaining implementation
- Works in conjunction with SaveTransactionCharacteristics to maintain transaction state continuity
- The function uses const qualifier for the parameter, indicating the saved structure is not modified
- Multiple calls within CommitTransactionCommandInternal suggest different code paths for transaction handling
- Located in src/backend/access/transam/xact.c:3080-3092
- Essential for maintaining transaction isolation and consistency across chained transactions