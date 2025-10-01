# DeleteSecurityLabel

## Location
[src/backend/commands/seclabel.c:523-569](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/seclabel.c#L523-L569)

## Overview
DeleteSecurityLabel removes all security labels for a specified database object and any sub-objects if applicable.

## Definition

```c
void
DeleteSecurityLabel(const ObjectAddress *object)
```
## Detailed Description
DeleteSecurityLabel removes all security labels associated with a database object from the appropriate system catalog. The function handles both regular objects (using pg_seclabel) and shared objects (delegating to DeleteSharedSecurityLabel for pg_shseclabel). For regular objects, it can delete labels for either a specific sub-object (when objectSubId is non-zero) or all sub-objects of the main object (when objectSubId is zero).

The function performs the following operations:
1. Checks if the object is a shared relation and delegates to DeleteSharedSecurityLabel if so
2. Sets up scan keys to locate security label entries for the target object
3. Uses either 2 or 3 scan keys depending on whether a specific sub-object is targeted
4. Scans through all matching tuples and deletes each one
5. Properly handles cleanup and lock management

## Parameters / Member Variables
- : Pointer to ObjectAddress structure identifying the target database object (contains classId, objectId, and objectSubId)

## Dependencies
- Functions called/Symbols referenced:
  - [IsSharedRelation](../I/IsSharedRelation.md)
  - [DeleteSharedSecurityLabel](DeleteSharedSecurityLabel.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
- Called from (representative examples):
  - [deleteOneObject](../d/deleteOneObject.md)

## Notes and Other Information
- The function handles both specific sub-object deletion (objectSubId != 0) and wholesale object deletion (objectSubId == 0)
- For shared objects, it asserts that objectSubId must be 0, as shared objects don't have sub-objects
- Uses different numbers of scan keys (2 or 3) depending on whether targeting a specific sub-object
- This function is typically called during object deletion as part of the dependency cleanup process
- Deletes ALL security labels for the object regardless of provider, making it suitable for complete cleanup
- The function is part of PostgreSQL's object deletion cascade system managed by the dependency subsystem

## Simplified Source

```c
void DeleteSecurityLabel(const ObjectAddress *object) {
    Relation pg_seclabel;
    ScanKeyData skey[3];
    SysScanDesc scan;
    HeapTuple oldtup;
    int nkeys;

    // Shared objects use different catalog - delegate to shared handler
    if (IsSharedRelation(object->classId)) {
        DeleteSharedSecurityLabel(object->objectId, object->classId);
        return;
    }

    // Set up scan keys for object and class OID
    ScanKeyInit(&skey[0], Anum_pg_seclabel_objoid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->objectId));
    ScanKeyInit(&skey[1], Anum_pg_seclabel_classoid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(object->classId));

    // Add sub-object key if needed
    if (object->objectSubId != 0) {
        ScanKeyInit(&skey[2], Anum_pg_seclabel_objsubid,
                    BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(object->objectSubId));
        nkeys = 3;  // Specific sub-object
    } else {
        nkeys = 2;  // All sub-objects
    }

    // Open pg_seclabel catalog table
    pg_seclabel = table_open(SecLabelRelationId, RowExclusiveLock);

    // Scan and delete all matching security labels
    scan = systable_beginscan(pg_seclabel, SecLabelObjectIndexId, true,
                             NULL, nkeys, skey);
    while (HeapTupleIsValid(oldtup = systable_getnext(scan))) {
        CatalogTupleDelete(pg_seclabel, &oldtup->t_self);
    }

    // Clean up
    systable_endscan(scan);
    table_close(pg_seclabel, RowExclusiveLock);
}
```