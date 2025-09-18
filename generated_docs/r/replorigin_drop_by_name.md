# replorigin_drop_by_name

## Location
src/backend/replication/logical/origin.c: 411 - 464

## Overview
Drops a replication origin by name from the PostgreSQL system catalog, removing both the catalog entry and clearing any associated replication state.

## Definition
```c
void replorigin_drop_by_name(const char *name, bool missing_ok, bool nowait)
```

## Detailed Description
This function removes a replication origin from the system by its name. It performs a comprehensive cleanup that includes:

1. Resolving the origin name to its internal ID using `replorigin_by_name()`
2. Acquiring an exclusive lock on the origin to prevent concurrent operations
3. Looking up the catalog entry in the system cache
4. Clearing the replication state associated with the origin
5. Deleting the catalog entry from pg_replication_origin
6. Incrementing the command counter to make the changes visible

The function must be called within a valid transaction context and maintains proper locking to ensure atomicity of the drop operation.

## Parameters / Member Variables
- `name`: The name of the replication origin to drop
- `missing_ok`: If true, do not raise an error if the origin does not exist; if false, raise an error when the origin is not found
- `nowait`: Parameter passed to `replorigin_state_clear()` to control blocking behavior when clearing replication state

## Dependencies
- Functions called/Symbols referenced:
  - [IsTransactionState](../I/IsTransactionState.md): Verifies the function is called within a transaction
  - `replorigin_by_name`: Resolves the origin name to its internal ID
  - [LockSharedObject](../L/LockSharedObject.md): Acquires exclusive lock on the origin
  - `replorigin_state_clear`: Clears replication state for the origin
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md): Removes the catalog entry
  - `CommandCounterIncrement`: Makes changes visible within the transaction
- Called from (representative examples):
  - [DropSubscription](../D/DropSubscription.md): When dropping logical replication subscriptions
  - [pg_replication_origin_drop](../p/pg_replication_origin_drop.md): SQL function wrapper for dropping origins
  - [process_syncing_tables_for_sync](../p/process_syncing_tables_for_sync.md): During table synchronization cleanup

## Notes and Other Information
- Must be called within a transaction context (enforced by `Assert(IsTransactionState())`)
- Uses AccessExclusiveLock to prevent concurrent modifications to the same origin
- Maintains the lock on pg_replication_origin until transaction commit
- If the origin is already dropped, locks are released early and the function returns gracefully
- The function is atomic - either the entire drop operation succeeds or it fails without partial state