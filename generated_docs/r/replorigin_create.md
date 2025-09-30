# replorigin_create

## Location
[src/backend/replication/logical/origin.c:252-340](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/origin.c#L252-L340)

## Overview
Creates a new replication origin with the specified name by finding an unused 16-bit identifier and inserting a new row into the pg_replication_origin system catalog.

## Definition

```c
RepOriginId
replorigin_create(const char *roname)
```
## Detailed Description
This function creates a new replication origin by performing a sequential search for an unused 16-bit origin identifier and inserting a corresponding catalog entry. The process uses an exclusive table lock to prevent concurrency issues while ensuring that newly created origins are immediately visible through dirty snapshot reads. The function enforces that replication origin IDs fit within 16-bit range (PG_UINT16_MAX) and uses a systematic approach to find the first available ID starting from InvalidOid + 1. The implementation prioritizes correctness over efficiency, as replication origin creation is expected to be an infrequent operation.

## Parameters / Member Variables
- : The name of the replication origin to create (null-terminated C string)

## Dependencies
- Functions called/Symbols referenced:
  - CStringGetTextDatum
  - [IsTransactionState](../I/IsTransactionState.md)
  - InitDirtySnapshot
  - [table_open](../t/table_open.md)
  - [table_close](../t/table_close.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - HeapTupleIsValid
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - CHECK_FOR_INTERRUPTS
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - RelationGetDescr
  - [SnapshotData](../S/SnapshotData.md)
  - [SysScanDesc](../S/SysScanDesc.md)
  - ExclusiveLock
  - PG_UINT16_MAX
- Called from (representative examples):
  - [CreateSubscription](../C/CreateSubscription.md)
  - [pg_replication_origin_create](../p/pg_replication_origin_create.md)
  - [LogicalRepSyncTableStart](../L/LogicalRepSyncTableStart.md)
  - [run_apply_worker](run_apply_worker.md)

## Notes and Other Information
- Must be called within a transaction context (asserted via IsTransactionState)
- Uses exclusive table locking to handle concurrency during ID allocation
- Employs dirty snapshots to see uncommitted changes from other transactions
- Searches sequentially for unused IDs from 1 to PG_UINT16_MAX (65535)
- Throws ERRCODE_PROGRAM_LIMIT_EXCEEDED if no free ID is available
- Calls CommandCounterIncrement to make the new origin immediately visible to subsequent operations
- The 16-bit ID limitation is a deliberate design choice for efficient storage and operations
- Returns the newly allocated RepOriginId on success

## Simplified Source

```c
RepOriginId
replorigin_create(const char *roname)
{
    Oid roident;
    HeapTuple tuple = NULL;
    Relation rel;
    Datum roname_d = CStringGetTextDatum(roname);
    SnapshotData SnapshotDirty;
    SysScanDesc scan;
    ScanKeyData key;

    Assert(IsTransactionState());

    // Use dirty snapshot to see uncommitted changes from other transactions
    InitDirtySnapshot(SnapshotDirty);

    // Lock table exclusively to prevent concurrent ID allocation
    rel = table_open(ReplicationOriginRelationId, ExclusiveLock);

    // Search for first unused ID in 16-bit range
    for (roident = InvalidOid + 1; roident < PG_UINT16_MAX; roident++) {
        CHECK_FOR_INTERRUPTS();

        // Check if this ID is already in use
        ScanKeyInit(&key, Anum_pg_replication_origin_roident,
                    BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(roident));

        scan = systable_beginscan(rel, ReplicationOriginIdentIndex, true,
                                  &SnapshotDirty, 1, &key);
        bool collides = HeapTupleIsValid(systable_getnext(scan));
        systable_endscan(scan);

        if (!collides) {
            // Found unused ID, create new catalog entry
            bool nulls[Natts_pg_replication_origin] = {0};
            Datum values[Natts_pg_replication_origin];

            values[Anum_pg_replication_origin_roident - 1] = ObjectIdGetDatum(roident);
            values[Anum_pg_replication_origin_roname - 1] = roname_d;

            tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
            CatalogTupleInsert(rel, tuple);
            CommandCounterIncrement();
            break;
        }
    }

    table_close(rel, ExclusiveLock);

    if (tuple == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
             errmsg("could not find free replication origin ID")));

    heap_freetuple(tuple);
    return roident;
}
```