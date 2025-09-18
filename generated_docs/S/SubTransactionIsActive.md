# SubTransactionIsActive

## Location
src/backend/access/transam/xact.c: 802 - 825

## Overview
Tests whether a specified subtransaction ID is still active within the current transaction hierarchy.

## Definition


## Detailed Description
SubTransactionIsActive traverses the current transaction state hierarchy to determine if a given subtransaction ID is still active. It walks up the parent chain of transaction states, starting from the current transaction state, and checks if any non-aborted transaction state matches the provided subtransaction ID. The function skips over aborted transaction states (TRANS_ABORT) since they are no longer considered active. This is essential for validating whether operations or resources associated with a particular subtransaction context are still valid.

## Parameters / Member Variables
- : The SubTransactionId to check for activity status

## Dependencies
- Functions called/Symbols referenced:
  - CurrentTransactionState (global variable)
  - TransactionState (struct type)
  - TRANS_ABORT (transaction state enum value)
  - SubTransactionId (type)
- Called from (representative examples):
  - [fmgr_sql](../f/fmgr_sql.md) (src/backend/executor/functions.c:1087)

## Notes and Other Information
- The caller is responsible for ensuring the provided subxid is relevant to the current transaction
- The function returns true if the subtransaction is found and active, false otherwise
- Aborted subtransactions are considered inactive and are skipped during the search
- Located in src/backend/access/transam/xact.c:802-825
- Part of PostgreSQL's nested transaction management system