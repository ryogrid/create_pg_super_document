# check_transaction_isolation

## Location
src/backend/commands/variable.c: 583 - 619

## Overview
This function validates changes to the transaction isolation level setting, ensuring that isolation level modifications comply with PostgreSQL's transaction management rules.

## Definition


## Detailed Description
 is a GUC check hook function that validates attempts to change the transaction isolation level via  commands. The function enforces PostgreSQL's transaction isolation semantics by allowing idempotent changes (setting the same isolation level) at any time, but restricting non-idempotent changes to top-level transactions that have not yet taken a snapshot.

Key restrictions enforced:
1. Isolation level changes are prohibited after the first snapshot is taken ()
2. Non-idempotent isolation level changes are not allowed in subtransactions
3. Serializable isolation level cannot be set during recovery (hot standby mode)

The function allows changes when not in an active transaction, similar to other transaction-related GUC settings.

## Parameters / Member Variables
- : Pointer to the new integer value representing the desired transaction isolation level
- : Pointer to extra data (unused in this function, can be NULL)
- : The source of the configuration change (GucSource enum)

## Dependencies
- Functions called/Symbols referenced:
  - [IsTransactionState](../I/IsTransactionState.md)
  - [IsSubTransaction](../I/IsSubTransaction.md)
  - GUC_check_errcode
  - GUC_check_errmsg
  - GUC_check_errhint
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - XACT_SERIALIZABLE (constant)
  - GucSource (enum type)
- Called from (representative examples):
  - GUC system via function pointer in guc_hooks.h

## Notes and Other Information
- This is a GUC check hook function for the  configuration parameter
- Uses global variables  and  to determine current transaction state
- Provides helpful error hints for serializable mode restrictions during recovery
- Returns  for valid isolation level changes,  for invalid ones
- The function specifically checks for serializable mode during recovery and suggests using REPEATABLE READ as an alternative
- Error handling includes specific error codes for different violation types (ERRCODE_ACTIVE_SQL_TRANSACTION, ERRCODE_FEATURE_NOT_SUPPORTED)