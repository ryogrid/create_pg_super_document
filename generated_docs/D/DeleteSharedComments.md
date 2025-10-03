# DeleteSharedComments

## Location
[src/backend/commands/comment.c:374-409](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/comment.c#L374-L409)

## Overview
Removes comments for cluster-wide shared objects from the pg_shdescription catalog table.

## Definition

```c
void
DeleteSharedComments(Oid oid, Oid classoid)
```
## Detailed Description
DeleteSharedComments removes comment entries from the pg_shdescription catalog table for cluster-wide shared objects such as databases, tablespaces, and roles. Unlike DeleteComments, this function operates on the shared description catalog and doesn't handle sub-object IDs since shared objects don't have sub-components. It performs a systematic scan using two-key lookups (object OID and class OID) and deletes all matching comment tuples.

The function is typically called during the dropping of shared objects to clean up their associated comments as part of the cascade deletion process.

## Parameters / Member Variables
- `oid`: Object identifier of the shared object whose comments should be deleted (database, tablespace, or role OID)
- `classoid`: OID of the system catalog containing the shared object (e.g., DatabaseRelationId, TableSpaceRelationId, AuthIdRelationId)
## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md): Opens the pg_shdescription relation for modification
  - [systable_beginscan](../s/systable_beginscan.md): Initiates indexed scan using SharedDescriptionObjIndexId
  - [systable_getnext](../s/systable_getnext.md): Iterates through matching comment tuples
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md): Removes each matching comment tuple
  - [systable_endscan](../s/systable_endscan.md): Ends the systematic scan
  - [table_close](../t/table_close.md): Closes the pg_shdescription relation
- Called from (representative examples):
  - [dropdb](../d/dropdb.md): Removes database comments when dropping a database
  - [DropTableSpace](DropTableSpace.md): Removes tablespace comments when dropping a tablespace
  - [DropRole](DropRole.md): Removes role comments when dropping a user/role

## Notes and Other Information
- Always uses exactly 2 scan keys since shared objects don't have sub-object identifiers
- Uses SharedDescriptionObjIndexId for efficient indexed lookups by (objoid, classoid)
- Acquires RowExclusiveLock on pg_shdescription during both open and close operations
- Simpler than DeleteComments due to lack of sub-object complexity
- Integral part of the cascade deletion process for shared objects

## Simplified Source

```c
void DeleteSharedComments(Oid oid, Oid classoid) {
    Relation shdescription;
    ScanKeyData skey[2];
    SysScanDesc sd;
    HeapTuple oldtuple;

    // Step 1: Set up scan keys for object and class OIDs
    ScanKeyInit(&skey[0], Anum_pg_shdescription_objoid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oid));
    ScanKeyInit(&skey[1], Anum_pg_shdescription_classoid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classoid));

    // Step 2: Open pg_shdescription catalog for modification
    shdescription = table_open(SharedDescriptionRelationId, RowExclusiveLock);

    // Step 3: Begin indexed scan to find matching comment entries
    sd = systable_beginscan(shdescription, SharedDescriptionObjIndexId, true,
                           NULL, 2, skey);

    // Step 4: Delete all matching comment tuples
    while ((oldtuple = systable_getnext(sd)) != NULL) {
        CatalogTupleDelete(shdescription, &oldtuple->t_self);
    }

    // Step 5: Clean up scan and close catalog
    systable_endscan(sd);
    table_close(shdescription, RowExclusiveLock);
}
```