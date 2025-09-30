# get_partition_parent_worker

## Location
[src/backend/catalog/partition.c:85-133](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/partition.c#L85-L133)

## Overview
A static helper function that scans the pg_inherits relation to return the OID of the parent of a given relation.

## Definition
```c
static Oid get_partition_parent_worker(Relation inhRel, Oid relid, bool *detach_pending)
```

## Detailed Description
This function performs the actual work of finding a partition's parent by scanning the pg_inherits system catalog. It uses a system scan with two scan keys: one to match the child relation OID (inhrelid) and another to match sequence number 1 (inhseqno), which represents the direct parent in the inheritance hierarchy. The function also detects if the partition is in the process of being detached by checking the inhdetachpending flag.

The function is designed to work with an already-opened pg_inherits relation, making it suitable for use by higher-level functions that manage the catalog access.

## Parameters / Member Variables
- `inhRel`: An already-opened Relation object for the pg_inherits catalog table
- `relid`: OID of the partition relation whose parent is to be found
- `detach_pending`: Output parameter that is set to true if the partition is being detached

## Dependencies
- Functions called/Symbols referenced:
  - [ScanKeyInit](../S/ScanKeyInit.md) (to initialize scan keys)
  - [systable_beginscan](../s/systable_beginscan.md) (to begin scanning pg_inherits)
  - [systable_getnext](../s/systable_getnext.md) (to get the next tuple)
  - HeapTupleIsValid (to validate the returned tuple)
  - GETSTRUCT (to extract the struct from the tuple)
  - [systable_endscan](../s/systable_endscan.md) (to end the scan)
  - Form_pg_inherits (struct type for pg_inherits tuples)

- Called from (representative examples):
  - [get_partition_parent](get_partition_parent.md)
  - [get_partition_ancestors_worker](get_partition_ancestors_worker.md)

## Notes and Other Information
- Uses InheritsRelidSeqnoIndexId for efficient indexed access
- The inhseqno = 1 condition ensures we get the direct parent (not grandparents)
- Sets *detach_pending to false initially and updates it if inhdetachpending flag is set
- Returns InvalidOid if no matching tuple is found
- Located at src/backend/catalog/partition.c:85-133
- Static function, only accessible within partition.c

## Simplified Source

```c
static Oid get_partition_parent_worker(Relation inhRel, Oid relid, bool *detach_pending) {
    Oid result = InvalidOid;
    *detach_pending = false;

    // Set up scan keys: match child relation ID and sequence number 1 (direct parent)
    ScanKeyInit(&key[0], Anum_pg_inherits_inhrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
    ScanKeyInit(&key[1], Anum_pg_inherits_inhseqno, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(1));

    // Scan pg_inherits using the index for efficiency
    scan = systable_beginscan(inhRel, InheritsRelidSeqnoIndexId, true, NULL, 2, key);
    tuple = systable_getnext(scan);

    if (HeapTupleIsValid(tuple)) {
        Form_pg_inherits form = (Form_pg_inherits) GETSTRUCT(tuple);

        // Check if partition is being detached
        if (form->inhdetachpending)
            *detach_pending = true;

        result = form->inhparent;
    }

    systable_endscan(scan);
    return result;
}
```