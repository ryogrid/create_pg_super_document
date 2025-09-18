# IsSubTransaction

## Location
src/backend/access/transam/xact.c: 4988 - 5010

## Overview
IsSubTransaction determines whether the current execution context is within a subtransaction by checking if the transaction nesting level is 2 or greater.

## Definition
```c
bool IsSubTransaction(void)
```

## Detailed Description
IsSubTransaction provides a simple check to determine if the current transaction context is a subtransaction rather than the top-level transaction. It examines the nestingLevel field of the current transaction state, returning true when the nesting level is 2 or higher. A nesting level of 1 represents the top-level transaction, while level 2 and above indicate subtransactions created through savepoints or other subtransaction mechanisms.

This function is essential for various PostgreSQL subsystems that need to behave differently when operating within subtransactions, such as SPI (Server Programming Interface) operations, trigger management, transaction property checks, and snapshot handling.

## Parameters / Member Variables
This function takes no parameters and returns a boolean value indicating whether the current context is within a subtransaction.

## Dependencies
- Functions called/Symbols referenced:
  - CurrentTransactionState (global variable)
  - TransactionState (type)
  - nestingLevel (field of TransactionState)
- Called from (representative examples):
  - PreventInTransactionBlock
  - CheckTransactionBlock
  - _SPI_commit
  - _SPI_rollback
  - SPI_inside_nonatomic_context
  - AfterTriggerSetState
  - ExportSnapshot
  - ImportSnapshot

## Notes and Other Information
The function uses a threshold of nestingLevel >= 2 to determine subtransaction status, where level 1 is the top-level transaction. This simple check is used throughout PostgreSQL to enforce different behaviors for subtransactions, such as preventing certain operations that are only allowed at the top-level transaction or enabling subtransaction-specific functionality like savepoint management. The function is particularly important for SPI operations and transaction property validation.