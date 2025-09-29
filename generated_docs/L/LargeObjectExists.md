# LargeObjectExists

## Location
[src/backend/catalog/pg_largeobject.c:155-184](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_largeobject.c#L155-L184)

## Overview
Checks whether a large object with the specified OID exists by scanning the pg_largeobject_metadata catalog table using a current snapshot.

## Definition
```c
bool LargeObjectExists(Oid loid)
```

## Detailed Description
The LargeObjectExists function determines if a large object exists by performing a system catalog scan on pg_largeobject_metadata. The function is designed with specific snapshot behavior considerations:

- It always uses an up-to-date snapshot when scanning the system catalog
- It does not use the system cache for large object metadata to avoid excessive local memory usage
- Should not be used when a large object is opened in read-only mode, as read-only mode operations should be relative to the caller's snapshot, while this function uses a current snapshot

The function performs a simple indexed lookup using the LargeObjectMetadataOidIndexId for efficient searching and returns true if a matching tuple is found, false otherwise. It uses AccessShareLock for the scan operation since it only needs to read the catalog.

## Parameters / Member Variables
- `loid`: The OID of the large object to check for existence. Must be a valid OID value.

## Dependencies
- Functions called/Symbols referenced:
  - [SysScanDesc](../S/SysScanDesc.md) (system scan descriptor type)
  - [systable_beginscan](../s/systable_beginscan.md) (begins system table scan)
  - [systable_getnext](../s/systable_getnext.md) (gets next tuple from system scan)
- Called from (representative examples):
  - [objectNamesToOids](../o/objectNamesToOids.md) (from src/backend/catalog/aclchk.c:735)
  - [get_object_address](../g/get_object_address.md) (from src/backend/catalog/objectaddress.c:1049)
  - [getObjectDescription](../g/getObjectDescription.md) (from src/backend/catalog/objectaddress.c:3134)
  - [getObjectIdentityParts](../g/getObjectIdentityParts.md) (from src/backend/catalog/objectaddress.c:5026)

## Notes and Other Information
- Returns boolean value: true if large object exists, false otherwise
- Uses AccessShareLock for non-blocking read access to the catalog
- Avoids system cache to prevent memory overhead
- Should not be used in read-only large object access contexts due to snapshot semantics
- Uses indexed scan for efficient lookup performance
- Part of the object address and ACL checking infrastructure
- Located in src/backend/catalog/pg_largeobject.c:155-184

## Simplified Source

```c
bool
LargeObjectExists(Oid loid)
{
    // Prepare scan key to search by OID
    ScanKeyData skey[1];
    ScanKeyInit(&skey[0], Anum_pg_largeobject_metadata_oid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(loid));

    // Open large object metadata table
    Relation pg_lo_meta = table_open(LargeObjectMetadataRelationId, AccessShareLock);

    // Start indexed scan using the OID index
    SysScanDesc sd = systable_beginscan(pg_lo_meta,
                                       LargeObjectMetadataOidIndexId, true,
                                       NULL, 1, skey);

    // Check if we found a matching tuple
    HeapTuple tuple = systable_getnext(sd);
    bool retval = HeapTupleIsValid(tuple);

    // Clean up scan and close table
    systable_endscan(sd);
    table_close(pg_lo_meta, AccessShareLock);

    return retval;
}
```