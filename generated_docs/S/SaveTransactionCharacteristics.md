# SaveTransactionCharacteristics

## Location
src/backend/access/transam/xact.c: 3072 - 3079

## Overview
SaveTransactionCharacteristics saves the current transaction's isolation level, read-only status, and deferrable property into a structure for later restoration, primarily used for transaction chaining.

## Definition
```c
void SaveTransactionCharacteristics(SavedTransactionCharacteristics *s)
```

## Detailed Description
SaveTransactionCharacteristics is a utility function designed to preserve transaction characteristics across transaction boundaries, specifically for transaction chaining functionality. The function captures three key transaction properties: isolation level, read-only mode, and deferrable status. This is necessary because PostgreSQL's GUC (Grand Unified Configuration) system automatically resets these characteristics at transaction end, so they must be explicitly saved and restored to maintain consistency in chained transactions.

The function performs a simple assignment operation, copying the current transaction's characteristics from global variables into the provided structure. This saved state can later be restored using RestoreTransactionCharacteristics to ensure the new transaction inherits the same properties as its predecessor.

## Parameters / Member Variables
- `s`: Pointer to SavedTransactionCharacteristics structure where the current transaction characteristics will be stored
  - `save_XactIsoLevel`: Stores the current transaction isolation level (XactIsoLevel)
  - `save_XactReadOnly`: Stores the current read-only transaction status (XactReadOnly)  
  - `save_XactDeferrable`: Stores the current deferrable transaction status (XactDeferrable)

## Dependencies
- Functions called/Symbols referenced:
  - SavedTransactionCharacteristics (structure)
  - XactIsoLevel (global variable)
  - XactReadOnly (global variable)
  - XactDeferrable (global variable)
- Called from (representative examples):
  - CommitTransactionCommandInternal
  - _SPI_commit
  - _SPI_rollback

## Notes and Other Information
- This function is part of PostgreSQL's transaction chaining implementation
- The saved characteristics must be restored using RestoreTransactionCharacteristics
- The GUC system's automatic reset behavior necessitates this explicit save/restore mechanism
- Located in src/backend/access/transam/xact.c:3072-3079
- Works in conjunction with RestoreTransactionCharacteristics to maintain transaction state continuity