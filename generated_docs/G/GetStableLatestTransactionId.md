# GetStableLatestTransactionId

## Location
src/backend/access/transam/xact.c: 604 - 631

## Overview
Returns a stable transaction ID reference point for the current transaction, either the transaction's own XID or the next-to-be-assigned XID, ensuring the same value is returned throughout the transaction's lifetime.

## Definition
```c
TransactionId GetStableLatestTransactionId(void)
```

## Detailed Description
This function provides a consistent reference point for transaction age calculations and other maintenance operations that require a stable transaction ID throughout a transaction's lifetime. It implements a caching mechanism using static variables to ensure the same value is returned for all calls within the same transaction.

The function works by:
1. Checking if the local transaction ID has changed (indicating a new transaction)
2. If in a new transaction, attempting to get the current transaction's XID using GetTopTransactionIdIfAny()
3. If no XID has been assigned to the current transaction, reading the next transaction ID that would be assigned
4. Caching this value using static variables keyed by the local transaction ID
5. Returning the cached value for subsequent calls within the same transaction

This design ensures that functions like age(xid) get consistent results even if called multiple times within a transaction, regardless of whether the transaction gets assigned an XID during its execution.

## Parameters / Member Variables
- No parameters (void function)
- Static variables used for caching:
  - `lxid`: Cached local transaction ID for comparison
  - `stablexid`: Cached stable transaction ID to return

## Dependencies
- Functions called/Symbols referenced:
  - LocalTransactionId (type)
  - InvalidLocalTransactionId
  - [GetTopTransactionIdIfAny](GetTopTransactionIdIfAny.md)
  - ReadNextTransactionId
- Called from (representative examples):
  - [xid_age](../x/xid_age.md)

## Notes and Other Information
- Primary use case is supporting the age(xid) SQL function
- Uses static variables for per-transaction caching
- Ensures consistent transaction age calculations within a single transaction
- Falls back to next available XID if current transaction has no assigned XID
- Essential for maintenance operations that need stable transaction reference points
- Located in src/backend/access/transam/xact.c:604-631