# binary_upgrade_add_sub_rel_state

## Location
src/backend/utils/adt/pg_upgrade_support.c: 325 - 368

## Overview
Adds a relation with specified replication state to the pg_subscription_rel catalog during binary upgrade operations.

## Definition
```c
Datum binary_upgrade_add_sub_rel_state(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of PostgreSQL's binary upgrade infrastructure for logical replication. It adds entries to the pg_subscription_rel catalog table, which tracks the replication state of individual relations (tables) within logical replication subscriptions.

The function performs the following operations:
1. Validates that required arguments are not null
2. Extracts subscription name, relation OID, replication state, and optional LSN from arguments
3. Opens the subscription relation catalog with exclusive locks
4. Resolves the subscription OID from the subscription name
5. Opens the target relation to validate it exists
6. Calls AddSubscriptionRelState to insert the relation state into pg_subscription_rel
7. Releases all acquired locks

This is specifically designed for binary upgrade scenarios where subscription relation states need to be preserved and restored during the upgrade process.

## Parameters / Member Variables
- `subname (text)`: Name of the subscription to add the relation state to
- `relid (Oid)`: Object identifier of the relation being added to subscription
- `relstate (char)`: Replication state character (e.g., 'i' for initializing, 'r' for ready, 's' for synchronized)
- `sublsn (XLogRecPtr, optional)`: LSN position for the subscription relation, can be null

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_IS_BINARY_UPGRADE
  - PG_ARGISNULL
  - text_to_cstring
  - PG_GETARG_TEXT_PP
  - PG_GETARG_OID
  - PG_GETARG_CHAR
  - PG_GETARG_LSN
  - table_open
  - get_subscription_oid
  - relation_open
  - AddSubscriptionRelState
  - relation_close
  - table_close
  - PG_RETURN_VOID
- Called from (representative examples):
  - No direct callers found (likely called via SQL during binary upgrades)

## Notes and Other Information
- This function is restricted to binary upgrade operations only via CHECK_IS_BINARY_UPGRADE
- The function includes null argument validation to prevent runtime errors
- Locks are released immediately since no concurrent subscription operations occur during upgrades
- The sublsn parameter is optional and defaults to InvalidXLogRecPtr if null
- Located in src/backend/utils/adt/pg_upgrade_support.c:325-368
- Critical for preserving logical replication subscription states during binary upgrades