# IsTransactionState

## Location
[src/backend/access/transam/xact.c:384-403](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L384-L403)

## Overview
Returns true if we are inside a valid transaction state where it is safe to initiate database access and take heavyweight locks.

## Definition
bool IsTransactionState(void)

## Detailed Description
IsTransactionState is a transaction state accessor function that determines whether the current transaction is in a valid state for performing database operations. The function checks the current transaction state and only returns true when the transaction is in the TRANS_INPROGRESS state.

The function explicitly rejects several states as unsafe:
- TRANS_DEFAULT: No transaction is active
- TRANS_ABORT: Transaction has been aborted
- TRANS_START: Transaction is starting (transition state)
- TRANS_COMMIT: Transaction is committing (transition state) 
- TRANS_PREPARE: Transaction is preparing for two-phase commit (transition state)

Only the TRANS_INPROGRESS state is considered safe for database operations, as it represents a stable, active transaction state.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - TransactionState (type for CurrentTransactionState)
  - TRANS_INPROGRESS (transaction state constant)
- Called from (representative examples):
  - [check_default_table_access_method](../c/check_default_table_access_method.md)
  - [SetMultiXactIdLimit](../S/SetMultiXactIdLimit.md)
  - [SetTransactionIdLimit](../S/SetTransactionIdLimit.md)
  - [IsSubxactTopXidLogPending](IsSubxactTopXidLogPending.md)
  - [check_transaction_read_only](../c/check_transaction_read_only.md)
  - [SearchCatCacheInternal](../S/SearchCatCacheInternal.md)
  - [RelationIdGetRelation](../R/RelationIdGetRelation.md)

## Notes and Other Information
- This is a fundamental safety check used throughout PostgreSQL to ensure operations are only performed within valid transaction contexts
- The function is particularly important for preventing operations during transaction state transitions
- Used extensively in GUC (Grand Unified Configuration) check functions and cache management
- Critical for logical replication and background worker processes to ensure they operate within proper transaction boundaries