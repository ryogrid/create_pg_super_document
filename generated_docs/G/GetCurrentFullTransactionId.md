# GetCurrentFullTransactionId

## Location
[src/backend/access/transam/xact.c:509-526](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L509-L526)

## Overview
Returns the full transaction ID of the current transaction (main or sub-transaction), assigning one if it hasn't been set yet.

## Definition
```c
FullTransactionId GetCurrentFullTransactionId(void)
```

## Detailed Description
This function retrieves the full transaction ID of the current transaction, whether it's a main transaction or a sub-transaction. If the current transaction doesn't have a full transaction ID assigned yet, it will automatically assign one by calling AssignTransactionId. The function ensures that a valid full transaction ID is always returned for the current transaction context.

The function works with the CurrentTransactionState to check if the transaction has a valid full transaction ID, and if not, it assigns one before returning it. This is different from the "IfAny" variants that don't force assignment.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - TransactionState (type)
  - CurrentTransactionState (global variable)
  - FullTransactionIdIsValid
  - [AssignTransactionId](../A/AssignTransactionId.md)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Must be called only inside a valid transaction context
- Automatically assigns a transaction ID if one doesn't exist for the current transaction
- Works with both main transactions and sub-transactions, unlike GetTopFullTransactionId which is specific to the top-level transaction
- Returns the full 64-bit transaction ID, not just the 32-bit XID portion
- Located in src/backend/access/transam/xact.c:509-526
- This function appears to be part of the internal transaction management API but may not have direct external callers in the current codebase