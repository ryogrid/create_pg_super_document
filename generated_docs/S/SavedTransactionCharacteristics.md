# SavedTransactionCharacteristics

## Location
src/include/access/xact.h: 152 - 157

## Overview
Data structure used to temporarily save and restore transaction characteristics (isolation level, read-only status, and deferrable status) for transaction chaining purposes.

## Definition


## Detailed Description
SavedTransactionCharacteristics is a simple data structure that provides a system for saving and restoring transaction characteristics. This is primarily needed for transaction chaining functionality, where the characteristics of a new transaction must match those of the previous transaction.

The structure works in conjunction with SaveTransactionCharacteristics() and RestoreTransactionCharacteristics() functions to preserve transaction state across transaction boundaries. This is necessary because the GUC (Grand Unified Configuration) system automatically resets transaction characteristics at transaction end, so simply skipping the reset in StartTransaction() would not be sufficient.

## Parameters / Member Variables
- : Stores the saved transaction isolation level (e.g., READ COMMITTED, SERIALIZABLE, etc.)
- : Stores the saved read-only status of the transaction
- : Stores the saved deferrable status of the transaction

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a data structure)
- Called from (representative examples):
  - SaveTransactionCharacteristics
  - RestoreTransactionCharacteristics
  - CommitTransactionCommandInternal
  - _SPI_commit
  - _SPI_rollback

## Notes and Other Information
- Located in src/include/access/xact.h:152-157
- Used primarily in transaction chaining scenarios where transaction characteristics must be preserved
- The structure is populated by SaveTransactionCharacteristics() which copies current global transaction state variables
- Values are restored by RestoreTransactionCharacteristics() which sets the global transaction state variables back to the saved values
- Essential for maintaining consistent transaction behavior across chained transactions in PostgreSQL