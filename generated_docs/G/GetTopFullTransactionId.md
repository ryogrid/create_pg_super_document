# GetTopFullTransactionId

## Location
src/backend/access/transam/xact.c: 480 - 495

## Overview
Returns the full transaction ID of the main (top-level) transaction, assigning one if it hasn't been set yet.

## Definition
```c
FullTransactionId GetTopFullTransactionId(void)
```

## Detailed Description
This function retrieves the full transaction ID of the top-level transaction. If the top-level transaction doesn't have a full transaction ID assigned yet, it will automatically assign one by calling AssignTransactionId. The function ensures that a valid full transaction ID is always returned for the main transaction.

The function works by checking if XactTopFullTransactionId is valid, and if not, it assigns a new transaction ID to the top transaction state before returning it.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionIdIsValid
  - AssignTransactionId
  - XactTopFullTransactionId (global variable)
  - TopTransactionStateData (global variable)
- Called from (representative examples):
  - pg_current_xact_id (src/backend/utils/adt/xid8funcs.c:344)

## Notes and Other Information
- Must be called only inside a valid transaction context
- Automatically assigns a transaction ID if one doesn't exist for the top-level transaction
- Returns the full 64-bit transaction ID, not just the 32-bit XID portion
- Located in src/backend/access/transam/xact.c:480-495
- Used primarily for SQL functions that need to expose the current transaction ID to users