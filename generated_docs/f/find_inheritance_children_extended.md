# find_inheritance_children_extended

## Location
[src/backend/catalog/pg_inherits.c:82-254](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_inherits.c#L82-L254)

## Overview
An extended version of find_inheritance_children that provides fine-grained control over handling detached partitions and returns additional information about detachment status.

## Definition
```c
List *find_inheritance_children_extended(Oid parentrelId, bool omit_detached,
                                       LOCKMODE lockmode, bool *detached_exist,
                                       TransactionId *detached_xmin)
```

## Detailed Description
This function is the core implementation for finding direct inheritance children of a relation. It extends the basic functionality of find_inheritance_children by providing sophisticated handling of partitions marked as "detach pending". The function scans pg_inherits to find all direct children and can optionally filter out detached partitions based on transaction visibility rules.

The function handles concurrent partition detachment scenarios by checking if detached partitions are visible to the active snapshot. This is crucial for maintaining consistency during REPEATABLE READ or SERIALIZABLE transactions, where different snapshots are used for RI queries versus regular user queries.

The function ensures consistent ordering by sorting child OIDs and implements proper locking protocols to avoid deadlocks when acquiring locks on multiple child relations.

## Parameters / Member Variables
- `parentrelId`: OID of the parent relation whose direct children should be found
- `omit_detached`: If true, omit partitions marked "detach pending" when they're not visible to the active snapshot
- `lockmode`: Lock mode to acquire on each child relation; use NoLock to skip locking
- `detached_exist`: Output parameter set to true if any detached partitions are found (can be NULL)
- `detached_xmin`: Output parameter set to the xmin of detached partition row (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [has_subclass](../h/has_subclass.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [ActiveSnapshotSet](../A/ActiveSnapshotSet.md)
  - HeapTupleHeaderGetXmin
  - [GetActiveSnapshot](../G/GetActiveSnapshot.md)
  - [XidInMVCCSnapshot](../X/XidInMVCCSnapshot.md)
  - [TransactionIdFollows](../T/TransactionIdFollows.md)
  - qsort (with oid_cmp)
  - [LockRelationOid](../L/LockRelationOid.md)
  - SearchSysCacheExists1
  - [UnlockRelationOid](../U/UnlockRelationOid.md)
  - [lappend_oid](../l/lappend_oid.md)
  - [repalloc](../r/repalloc.md)
- Called from (representative examples):
  - [find_inheritance_children](find_inheritance_children.md) (src/backend/catalog/pg_inherits.c:60)
  - [RelationBuildPartitionDesc](../R/RelationBuildPartitionDesc.md) (src/backend/partitioning/partdesc.c:164)

## Notes and Other Information
- Uses an optimization where it skips the scan entirely if has_subclass() indicates no children exist
- Implements sophisticated transaction visibility logic for detached partitions using snapshot comparison
- Sorts child OIDs to ensure consistent locking order and avoid deadlocks
- Handles concurrent drops by double-checking relation existence after acquiring locks
- Warns if multiple detached partitions are found (which shouldn't normally occur)
- When detached_xmin conflicts occur, tracks the newer transaction ID using TransactionIdFollows
- The detached partition handling is specifically designed for partition management operations where transaction isolation levels matter
- Located in src/backend/catalog/pg_inherits.c:82-254

## Simplified Source

```c
List *
find_inheritance_children_extended(Oid parentrelId, bool omit_detached,
                                   LOCKMODE lockmode, bool *detached_exist,
                                   TransactionId *detached_xmin)
{
    List *list = NIL;
    Relation relation;
    SysScanDesc scan;
    ScanKeyData key[1];
    HeapTuple inheritsTuple;
    Oid *oidarr;
    int maxoids, numoids, i;

    // Optimization: skip scan if parent has no subclasses
    if (!has_subclass(parentrelId))
        return NIL;

    // Initialize working array for child OIDs
    maxoids = 32;
    oidarr = (Oid *) palloc(maxoids * sizeof(Oid));
    numoids = 0;

    // Scan pg_inherits for direct children
    relation = table_open(InheritsRelationId, AccessShareLock);
    ScanKeyInit(&key[0], Anum_pg_inherits_inhparent,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(parentrelId));
    scan = systable_beginscan(relation, InheritsParentIndexId, true,
                             NULL, 1, key);

    while ((inheritsTuple = systable_getnext(scan)) != NULL)
    {
        Form_pg_inherits inhform = (Form_pg_inherits) GETSTRUCT(inheritsTuple);

        // Handle detached partitions
        if (inhform->inhdetachpending)
        {
            if (detached_exist)
                *detached_exist = true;

            // Check if we should omit this detached partition
            if (omit_detached && ActiveSnapshotSet())
            {
                TransactionId xmin = HeapTupleHeaderGetXmin(inheritsTuple->t_data);
                Snapshot snap = GetActiveSnapshot();

                if (!XidInMVCCSnapshot(xmin, snap))
                {
                    // Track detached xmin for caller
                    if (detached_xmin)
                        *detached_xmin = xmin;
                    continue; // Skip this partition
                }
            }
        }

        // Add child to array, expanding if needed
        Oid inhrelid = inhform->inhrelid;
        if (numoids >= maxoids)
        {
            maxoids *= 2;
            oidarr = (Oid *) repalloc(oidarr, maxoids * sizeof(Oid));
        }
        oidarr[numoids++] = inhrelid;
    }

    systable_endscan(scan);
    table_close(relation, AccessShareLock);

    // Sort children by OID for consistent lock ordering
    if (numoids > 1)
        qsort(oidarr, numoids, sizeof(Oid), oid_cmp);

    // Acquire locks and build result list
    for (i = 0; i < numoids; i++)
    {
        Oid inhrelid = oidarr[i];

        if (lockmode != NoLock)
        {
            // Lock child relation
            LockRelationOid(inhrelid, lockmode);

            // Double-check relation still exists after locking
            if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(inhrelid)))
            {
                UnlockRelationOid(inhrelid, lockmode);
                continue;
            }
        }

        list = lappend_oid(list, inhrelid);
    }

    pfree(oidarr);
    return list;
}
```