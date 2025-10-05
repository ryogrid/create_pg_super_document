# myLargeObjectExists

## Location
[src/backend/storage/large_object/inv_api.c:131-168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/large_object/inv_api.c#L131-L168)

## Overview
Checks whether a large object with a given OID exists in the pg_largeobject_metadata catalog, using a specified snapshot for the visibility check.

## Definition

```c
struct varlena *) datafield);
```
## Detailed Description
This internal function provides a snapshot-aware version of large object existence checking, similar to the LargeObjectExists() function in pg_largeobject.c but with the ability to specify a custom snapshot for visibility. It performs a system catalog scan on the pg_largeobject_metadata relation to search for a tuple with the specified large object OID. The function uses the system scan infrastructure with a B-tree equal strategy to efficiently locate the metadata record. It opens the metadata relation with AccessShareLock, performs the scan, and returns true if a valid tuple is found.

## Parameters / Member Variables
- : The OID of the large object to check for existence
- : The snapshot to use for visibility determination during the catalog scan

## Dependencies
- Functions called/Symbols referenced:
  - [ScanKeyInit](../S/ScanKeyInit.md) (to initialize scan key)
  - [table_open](../t/table_open.md) (to open pg_largeobject_metadata relation)
  - [systable_beginscan](../s/systable_beginscan.md) (to begin system catalog scan)
  - [systable_getnext](../s/systable_getnext.md) (to retrieve next tuple from scan)
  - [systable_endscan](../s/systable_endscan.md) (to end the scan)
  - [table_close](../t/table_close.md) (to close the relation)
- Called from (representative examples):
  - [inv_open](../i/inv_open.md)

## Notes and Other Information
- Function is static (internal to inv_api.c)
- Uses AccessShareLock for reading the metadata relation
- Employs system catalog scanning infrastructure for efficient OID lookup
- Returns boolean result indicating existence
- Key difference from LargeObjectExists() is the configurable snapshot parameter
- Properly manages relation opening/closing and scan lifecycle

## Simplified Source

```c
static bool myLargeObjectExists(Oid loid, Snapshot snapshot) {
    ScanKeyData skey[1];
    bool retval = false;

    // Set up scan key to search for the large object OID
    ScanKeyInit(&skey[0],
                Anum_pg_largeobject_metadata_oid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(loid));

    // Open large object metadata relation for reading
    Relation pg_lo_meta = table_open(LargeObjectMetadataRelationId, AccessShareLock);

    // Begin system catalog scan using specified snapshot
    SysScanDesc sd = systable_beginscan(pg_lo_meta,
                                      LargeObjectMetadataOidIndexId, true,
                                      snapshot, 1, skey);

    // Check if a matching tuple exists
    HeapTuple tuple = systable_getnext(sd);
    if (HeapTupleIsValid(tuple))
        retval = true;

    // Clean up scan and relation
    systable_endscan(sd);
    table_close(pg_lo_meta, AccessShareLock);

    return retval;
}
```