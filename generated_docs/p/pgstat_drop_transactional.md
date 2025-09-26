# pgstat_drop_transactional

## Location
src/backend/utils/activity/pgstat_xact.c: 379 - 382

## Overview
Drops a statistics entry for a database object that has been dropped, in a transactional manner, ensuring that if the current transaction or sub-transaction aborts, the stats entry will remain alive.

## Definition
```c
void pgstat_drop_transactional(PgStat_Kind kind, Oid dboid, Oid objoid)
```

## Detailed Description
This function marks a statistics entry for deletion when a database object (relation, function, subscription, etc.) is dropped, but does so with transactional semantics. The key behavior is that if the current transaction or sub-transaction is aborted, the statistics entry will NOT be dropped and will remain available.

This function is the counterpart to `pgstat_create_transactional` and provides the other half of transactional statistics management. It simply delegates to `create_drop_transactional_internal` with the `create` parameter set to false, indicating this is a drop operation.

The function registers the drop operation in a transaction-local pending operations list. The actual deletion of the statistics entry is deferred until transaction commit, ensuring that the statistics remain accessible until the transaction successfully completes.

## Parameters / Member Variables
- `kind`: The type of PostgreSQL statistics object being dropped (e.g., relation, function, subscription)
- `dboid`: The OID of the database containing the object
- `objoid`: The OID of the specific object whose statistics are being dropped

## Dependencies
- Functions called/Symbols referenced:
  - create_drop_transactional_internal
  - PgStat_Kind (type)
- Called from (representative examples):
  - pgstat_drop_database
  - pgstat_drop_function
  - pgstat_drop_relation
  - pgstat_drop_subscription

## Notes and Other Information
- Located in src/backend/utils/activity/pgstat_xact.c:379-382
- Much simpler than `pgstat_create_transactional` as it only needs to register the pending drop operation
- The transactional nature means the statistics deletion is tied to the success of the current transaction
- Works in conjunction with `pgstat_create_transactional` to provide complete transactional semantics for statistics management
- The actual statistics entry remains accessible until transaction commit, allowing for potential rollback scenarios
- Uses the same underlying mechanism (`create_drop_transactional_internal`) as the create function but with opposite semantics