# vac_update_datfrozenxid

## Location
[src/backend/commands/vacuum.c:1585-1803](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuum.c#L1585-L1803)

## Overview
Updates the database-wide frozen transaction ID and minimum MultiXactId values in pg_database by scanning all relations and finding the minimum values, then attempts to truncate transaction logs if advancement is possible.

## Definition

```c
void
vac_update_datfrozenxid(void)
```
## Detailed Description
This function performs a critical database maintenance task by updating the datfrozenxid and datminmxid fields in the pg_database system catalog. The process involves:

1. **Database-level Locking**: Acquires an exclusive lock to prevent concurrent updates that could cause the values to move backward
2. **Minimum Value Calculation**: Scans all pg_class entries to find the minimum relfrozenxid and relminmxid values across all relations
3. **Safety Validation**: Checks for "future" transaction IDs that might indicate corruption and abandons the operation if found
4. **Database Tuple Update**: Uses in-place updates to modify the pg_database tuple, avoiding transaction semantic issues similar to vac_update_relstats
5. **Log Truncation**: Attempts to truncate pg_xact and pg_multixact logs if advancement was achieved

The function employs conservative initialization values and validates all discovered transaction IDs to ensure database consistency. It uses the same in-place update mechanism as vac_update_relstats to avoid leaving dead tuples in system catalogs.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [LockDatabaseFrozenIds](../L/LockDatabaseFrozenIds.md)
  - [GetOldestNonRemovableTransactionId](../G/GetOldestNonRemovableTransactionId.md)
  - [GetOldestMultiXactId](../G/GetOldestMultiXactId.md)
  - [ReadNextTransactionId](../R/ReadNextTransactionId.md)
  - [ReadNextMultiXactId](../R/ReadNextMultiXactId.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_inplace_update_begin](../s/systable_inplace_update_begin.md)
  - [systable_inplace_update_finish](../s/systable_inplace_update_finish.md)
  - [systable_inplace_update_cancel](../s/systable_inplace_update_cancel.md)
  - TransactionIdIsNormal
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - MultiXactIdIsValid
  - [MultiXactIdPrecedes](../M/MultiXactIdPrecedes.md)
  - [ForceTransactionIdLimitUpdate](../F/ForceTransactionIdLimitUpdate.md)
  - [vac_truncate_clog](vac_truncate_clog.md)
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [vacuum](vacuum.md)
  - [do_autovacuum](../d/do_autovacuum.md)

## Notes and Other Information
- Only considers relations that can hold unfrozen XIDs (RELKIND_RELATION, RELKIND_MATVIEW, RELKIND_TOASTVALUE)
- Implements a "chicken out" mechanism when bogus (future) transaction IDs are detected to prevent premature log truncation
- The exclusive database lock prevents race conditions where multiple backends might compute different minimum values
- Transaction ID advancement is conditional - the function will not move datfrozenxid backward unless the current value appears corrupt
- Some table access methods may not require per-relation XID/MultiXact horizons, so Invalid values are handled appropriately
- The function triggers log truncation even when ForceTransactionIdLimitUpdate() indicates stale shared XID-wrap-limit information
- Uses sequential scan of pg_class since no suitable index exists for finding minimum transaction ID values

## Simplified Source

```c
void vac_update_datfrozenxid(void) {
    HeapTuple tuple;
    Form_pg_database dbform;
    Relation relation;
    SysScanDesc scan;
    HeapTuple classTup;
    TransactionId newFrozenXid;
    MultiXactId newMinMulti;
    TransactionId lastSaneFrozenXid;
    MultiXactId lastSaneMinMulti;
    bool bogus = false;
    bool dirty = false;
    ScanKeyData key[1];
    void *inplace_state;

    // Lock database to prevent concurrent updates
    LockDatabaseFrozenIds(ExclusiveLock);

    // Initialize minimum values with conservative estimates
    newFrozenXid = GetOldestNonRemovableTransactionId(NULL);
    newMinMulti = GetOldestMultiXactId();
    lastSaneFrozenXid = ReadNextTransactionId();
    lastSaneMinMulti = ReadNextMultiXactId();

    // Scan all relations to find minimum frozen XIDs
    relation = table_open(RelationRelationId, AccessShareLock);
    scan = systable_beginscan(relation, InvalidOid, false, NULL, 0, NULL);

    while ((classTup = systable_getnext(scan)) != NULL) {
        volatile FormData_pg_class *classForm = (Form_pg_class) GETSTRUCT(classTup);
        TransactionId relfrozenxid = classForm->relfrozenxid;
        TransactionId relminmxid = classForm->relminmxid;

        // Only consider relations that can hold unfrozen XIDs
        if (classForm->relkind != RELKIND_RELATION &&
            classForm->relkind != RELKIND_MATVIEW &&
            classForm->relkind != RELKIND_TOASTVALUE) {
            continue;
        }

        // Check for valid frozen XID and update minimum
        if (TransactionIdIsValid(relfrozenxid)) {
            // Detect future XIDs (corruption indicator)
            if (TransactionIdPrecedes(lastSaneFrozenXid, relfrozenxid)) {
                bogus = true;
                break;
            }
            if (TransactionIdPrecedes(relfrozenxid, newFrozenXid))
                newFrozenXid = relfrozenxid;
        }

        // Similar logic for MultiXact IDs
        if (MultiXactIdIsValid(relminmxid)) {
            if (MultiXactIdPrecedes(lastSaneMinMulti, relminmxid)) {
                bogus = true;
                break;
            }
            if (MultiXactIdPrecedes(relminmxid, newMinMulti))
                newMinMulti = relminmxid;
        }
    }

    systable_endscan(scan);
    table_close(relation, AccessShareLock);

    // Abort if corruption detected
    if (bogus)
        return;

    // Update pg_database with new minimum values
    relation = table_open(DatabaseRelationId, RowExclusiveLock);

    ScanKeyInit(&key[0], Anum_pg_database_oid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(MyDatabaseId));

    systable_inplace_update_begin(relation, DatabaseOidIndexId, true,
                                  NULL, 1, key, &tuple, &inplace_state);

    dbform = (Form_pg_database) GETSTRUCT(tuple);

    // Update datfrozenxid if advancement possible or current value is corrupt
    if (dbform->datfrozenxid != newFrozenXid &&
        (TransactionIdPrecedes(dbform->datfrozenxid, newFrozenXid) ||
         TransactionIdPrecedes(lastSaneFrozenXid, dbform->datfrozenxid))) {
        dbform->datfrozenxid = newFrozenXid;
        dirty = true;
    } else {
        newFrozenXid = dbform->datfrozenxid;
    }

    // Similar update for datminmxid
    if (dbform->datminmxid != newMinMulti &&
        (MultiXactIdPrecedes(dbform->datminmxid, newMinMulti) ||
         MultiXactIdPrecedes(lastSaneMinMulti, dbform->datminmxid))) {
        dbform->datminmxid = newMinMulti;
        dirty = true;
    } else {
        newMinMulti = dbform->datminmxid;
    }

    // Commit or cancel the update
    if (dirty)
        systable_inplace_update_finish(inplace_state, tuple);
    else
        systable_inplace_update_cancel(inplace_state);

    heap_freetuple(tuple);
    table_close(relation, RowExclusiveLock);

    // Attempt log truncation if values advanced or limits are stale
    if (dirty || ForceTransactionIdLimitUpdate())
        vac_truncate_clog(newFrozenXid, newMinMulti,
                         lastSaneFrozenXid, lastSaneMinMulti);
}
```