# TransactionStartedDuringRecovery

## Location
src/backend/access/transam/xact.c: 1039 - 1047

## Overview
TransactionStartedDuringRecovery determines whether the current transaction was initiated while the PostgreSQL server was still in recovery mode.

## Definition

```c
bool
TransactionStartedDuringRecovery(void)
```
## Detailed Description
This function provides a way to check if the currently active transaction began while PostgreSQL was still performing crash recovery or standby recovery. It returns the value stored in the current transaction state's  flag, which is set when a transaction begins during recovery mode.

The function is particularly important because recovery mode might have completed since the transaction started, meaning that  could return false even though this transaction was initiated during recovery. This distinction is crucial for maintaining proper transaction semantics and ensuring that operations behave correctly based on the recovery state at transaction start time.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - CurrentTransactionState (global transaction state variable)
- Called from (representative examples):
  - RelationGetIndexScan

## Notes and Other Information
- Located in src/backend/access/transam/xact.c:1031-1042
- The function simply returns a boolean flag from the current transaction state
- This check is essential for maintaining consistency in transaction behavior across recovery state transitions
- Used to ensure that certain operations are handled appropriately based on whether they were initiated during recovery