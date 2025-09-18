# pg_current_xact_id

## Location
src/backend/utils/adt/xid8funcs.c: 334 - 351

## Overview
A PostgreSQL built-in function that returns the current toplevel full transaction ID (xid8), assigning one if the current transaction doesn't already have one.

## Definition
```c
Datum pg_current_xact_id(PG_FUNCTION_ARGS)
```

## Detailed Description
This function provides a way to retrieve the full transaction ID of the current toplevel transaction. Unlike the older pg_current_xact_id_if_assigned function, this function will always return a valid transaction ID by assigning one if necessary. The function includes safety measures to prevent execution during recovery, as transaction ID assignment would fail in that context and programs depend on this function to always return a valid transaction ID.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro (no specific parameters for this function)

## Dependencies
- Functions called/Symbols referenced:
  - PreventCommandDuringRecovery
  - GetTopFullTransactionId
  - PG_RETURN_FULLTRANSACTIONID
- Called from (representative examples):
  - No direct references found (called via SQL function interface)

## Notes and Other Information
- This is a SQL-callable function exposed to PostgreSQL users
- Always returns a valid transaction ID, assigning one if none exists for the current transaction  
- Cannot be called during recovery operations due to transaction assignment restrictions
- Returns the toplevel transaction ID, not a subtransaction ID
- Part of PostgreSQL's xid8 (8-byte transaction ID) function family
- Replaces the need to check if a transaction ID exists before retrieving it
- Located in src/backend/utils/adt/xid8funcs.c as part of the extended transaction ID support