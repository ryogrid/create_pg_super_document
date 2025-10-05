# clear_subscription_skip_lsn

## Location
[src/backend/replication/logical/worker.c:4880-4968](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L4880-L4968)

## Overview
Clears the subskiplsn field in the pg_subscription catalog for the current subscription, with validation to ensure the skip LSN matches the expected finish LSN of a transaction.

## Definition
static void clear_subscription_skip_lsn(XLogRecPtr finish_lsn)

## Detailed Description
This function is responsible for clearing the subskiplsn (subscription skip LSN) field in the pg_subscription catalog table. The subskiplsn is used in logical replication to skip problematic transactions during replication. When a transaction completes successfully or needs to be cleared, this function removes the skip LSN setting.

The function performs several important validations and safety checks:
- It only operates if there is a valid skip LSN set and the worker is not a parallel apply worker
- It manages transaction state, starting a new transaction if needed
- It uses proper locking to prevent concurrent modifications of the subscription
- It validates that the current skip LSN matches the provided finish_lsn before clearing it
- If the LSNs don't match, it issues a warning to inform users of potential mismatches

The function follows PostgreSQL's standard patterns for catalog updates, including proper snapshot management, locking, and tuple modification procedures.

## Parameters / Member Variables
- : The LSN of the transaction that has finished processing, used to validate against the current subskiplsn before clearing it

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecPtrIsInvalid (checks if LSN is invalid)
  - [am_parallel_apply_worker](../a/am_parallel_apply_worker.md) (checks if this is a parallel worker)
  - [IsTransactionState](../I/IsTransactionState.md) (checks transaction state)
  - [StartTransactionCommand](../S/StartTransactionCommand.md) (starts new transaction if needed)
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md)/PushActiveSnapshot (snapshot management)
  - [LockSharedObject](../L/LockSharedObject.md) (prevents concurrent subscription updates)
  - SearchSysCacheCopy1 (retrieves subscription tuple)
  - [heap_modify_tuple](../h/heap_modify_tuple.md) (modifies the catalog tuple)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (updates the catalog)
  - [heap_freetuple](../h/heap_freetuple.md) (frees tuple memory)
  - [PopActiveSnapshot](../P/PopActiveSnapshot.md) (pops snapshot)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md) (commits transaction if started)
- Called from (representative examples):
  - [apply_handle_prepare](../a/apply_handle_prepare.md)
  - [apply_handle_commit_prepared](../a/apply_handle_commit_prepared.md)
  - [apply_handle_rollback_prepared](../a/apply_handle_rollback_prepared.md)
  - [apply_handle_stream_prepare](../a/apply_handle_stream_prepare.md)
  - [apply_handle_commit_internal](../a/apply_handle_commit_internal.md)

## Notes and Other Information
- This is a static function only used within worker.c
- The function handles cases where the skip LSN might have been changed by another process, issuing warnings but not failing
- Proper transaction and snapshot management ensures data consistency
- The function skips operation for parallel apply workers as they don't manage subscription state directly
- Located in src/backend/replication/logical/worker.c:4880-4968

## Simplified Source

```c
static void
clear_subscription_skip_lsn(XLogRecPtr finish_lsn)
{
    XLogRecPtr myskiplsn = MySubscription->skiplsn;
    bool started_tx = false;

    // Skip if no skip LSN set or parallel worker
    if (likely(XLogRecPtrIsInvalid(myskiplsn)) || am_parallel_apply_worker())
        return;

    // Start transaction if needed
    if (!IsTransactionState()) {
        StartTransactionCommand();
        started_tx = true;
    }

    // Set up snapshot for catalog access
    PushActiveSnapshot(GetTransactionSnapshot());

    // Lock subscription to prevent concurrent updates
    LockSharedObject(SubscriptionRelationId, MySubscription->oid, 0, AccessShareLock);

    // Open subscription catalog and fetch current tuple
    Relation rel = table_open(SubscriptionRelationId, RowExclusiveLock);
    HeapTuple tup = SearchSysCacheCopy1(SUBSCRIPTIONOID,
                                        ObjectIdGetDatum(MySubscription->oid));

    if (!HeapTupleIsValid(tup))
        elog(ERROR, "subscription \"%s\" does not exist", MySubscription->name);

    Form_pg_subscription subform = (Form_pg_subscription) GETSTRUCT(tup);

    // Clear skip LSN if it matches current value
    if (subform->subskiplsn == myskiplsn) {
        // Prepare tuple update to clear subskiplsn
        bool nulls[Natts_pg_subscription] = {false};
        bool replaces[Natts_pg_subscription] = {false};
        Datum values[Natts_pg_subscription] = {0};

        values[Anum_pg_subscription_subskiplsn - 1] = LSNGetDatum(InvalidXLogRecPtr);
        replaces[Anum_pg_subscription_subskiplsn - 1] = true;

        // Update catalog
        tup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces);
        CatalogTupleUpdate(rel, &tup->t_self, tup);

        // Warn if LSN mismatch
        if (myskiplsn != finish_lsn)
            ereport(WARNING,
                    errmsg("skip-LSN of subscription \"%s\" cleared", MySubscription->name),
                    errdetail("Remote transaction's finish WAL location (LSN) %X/%X did not match skip-LSN %X/%X.",
                              LSN_FORMAT_ARGS(finish_lsn), LSN_FORMAT_ARGS(myskiplsn)));
    }

    // Cleanup
    heap_freetuple(tup);
    table_close(rel, NoLock);
    PopActiveSnapshot();

    if (started_tx)
        CommitTransactionCommand();
}
```