# pg_current_xact_id_if_assigned

## Location
src/backend/utils/adt/xid8funcs.c: 352 - 369

## Overview
A PostgreSQL built-in function that returns the current toplevel full transaction ID (xid8) only if one is already assigned, returning NULL otherwise.

## Definition
```c
Datum pg_current_xact_id_if_assigned(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides a non-intrusive way to check the current toplevel transaction ID without forcing assignment of a new transaction ID. Unlike pg_current_xact_id(), this function will return NULL if the current transaction has not yet been assigned a transaction ID, making it useful for cases where you want to query the transaction ID without affecting transaction state or performance. This function is safe to call during recovery since it never attempts to assign new transaction IDs.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro (no specific parameters for this function)

## Dependencies
- Functions called/Symbols referenced:
  - [GetTopFullTransactionIdIfAny](../G/GetTopFullTransactionIdIfAny.md)
  - FullTransactionIdIsValid
  - PG_RETURN_FULLTRANSACTIONID
  - PG_RETURN_NULL
- Types referenced:
  - FullTransactionId
- Called from (representative examples):
  - No direct references found (called via SQL function interface)

## Notes and Other Information
- This is a SQL-callable function exposed to PostgreSQL users
- Returns NULL if no transaction ID has been assigned to the current transaction
- Does not trigger transaction ID assignment, making it performance-neutral
- Safe to call during recovery operations unlike pg_current_xact_id()
- Returns the toplevel transaction ID, not a subtransaction ID
- Part of PostgreSQL's xid8 (8-byte transaction ID) function family
- Useful for monitoring or diagnostic purposes where transaction ID assignment should be avoided
- Located in src/backend/utils/adt/xid8funcs.c as part of the extended transaction ID support