# DeleteSharedSecurityLabel

## Location
[src/backend/commands/seclabel.c:491-522](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/seclabel.c#L491-L522)

## Overview
DeleteSharedSecurityLabel removes all security labels associated with a specified shared database object.

## Definition

```c
void
DeleteSharedSecurityLabel(Oid objectId, Oid classId)
```
## Detailed Description
DeleteSharedSecurityLabel is a helper function of DeleteSecurityLabel specifically designed to handle shared database objects. Shared objects in PostgreSQL are those that exist at the cluster level rather than within individual databases, such as roles, tablespaces, and databases themselves. The function deletes all security label entries for the specified object from the pg_shseclabel system catalog.

The function performs the following operations:
1. Opens the pg_shseclabel system catalog with RowExclusiveLock
2. Sets up a scan using the SharedSecLabelObjectIndexId index to find all entries matching the object
3. Iterates through all matching tuples and deletes each one using CatalogTupleDelete
4. Closes the catalog and releases the lock

## Parameters / Member Variables
- `objectId`: The OID of the shared database object whose security labels are to be deleted
- `classId`: The OID of the system catalog class that the object belongs to (e.g., AuthIdRelationId for roles)
## Dependencies
- Functions called/Symbols referenced:
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
- Called from (representative examples):
  - [dropdb](../d/dropdb.md)
  - [DeleteSecurityLabel](DeleteSecurityLabel.md)
  - [DropTableSpace](DropTableSpace.md)
  - [DropRole](DropRole.md)

## Notes and Other Information
- This function is specifically for shared objects - regular database objects use DeleteSecurityLabel instead
- The function deletes ALL security labels for the object regardless of provider, making it suitable for cleanup during object deletion
- Uses a while loop to handle cases where an object might have multiple security labels from different providers
- The function is typically called during DROP operations for shared objects to ensure proper cleanup
- No return value since this is a cleanup operation that should always succeed or raise an error

## Simplified Source

```c
void DeleteSharedSecurityLabel(Oid objectId, Oid classId) {
    Relation pg_shseclabel;
    ScanKeyData skey[2];
    SysScanDesc scan;
    HeapTuple oldtup;

    // Step 1: Set up scan keys for object and class OIDs
    ScanKeyInit(&skey[0], Anum_pg_shseclabel_objoid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(objectId));
    ScanKeyInit(&skey[1], Anum_pg_shseclabel_classoid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classId));

    // Step 2: Open pg_shseclabel catalog for modification
    pg_shseclabel = table_open(SharedSecLabelRelationId, RowExclusiveLock);

    // Step 3: Begin indexed scan to find matching security label entries
    scan = systable_beginscan(pg_shseclabel, SharedSecLabelObjectIndexId, true,
                             NULL, 2, skey);

    // Step 4: Delete all matching security label tuples
    while (HeapTupleIsValid(oldtup = systable_getnext(scan))) {
        CatalogTupleDelete(pg_shseclabel, &oldtup->t_self);
    }

    // Step 5: Clean up scan and close catalog
    systable_endscan(scan);
    table_close(pg_shseclabel, RowExclusiveLock);
}
```