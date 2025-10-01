# DeleteComments

## Location
[src/backend/commands/comment.c:326-373](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/comment.c#L326-L373)

## Overview
Removes comments from the pg_description catalog table for specified database objects, with support for deleting specific sub-object comments or all comments for an entire object.

## Definition

```c
void
DeleteComments(Oid oid, Oid classoid, int32 subid)
```
## Detailed Description
DeleteComments removes comment entries from the pg_description catalog table based on the provided object identifiers. It supports two deletion modes: when subid is nonzero, it deletes only comments for that specific sub-object (e.g., a specific column); when subid is zero, it deletes all comments associated with the object regardless of sub-object ID (used when dropping entire objects).

The function performs a systematic scan using the DescriptionObjIndexId and deletes all matching tuples. It uses variable-length scan keys depending on whether sub-object specificity is required.

## Parameters / Member Variables
- : Object identifier of the target database object whose comments should be deleted
- : OID of the system catalog containing the object (e.g., RelationRelationId for tables)
- : Sub-object identifier - if nonzero, deletes only comments for this specific sub-object; if zero, deletes all comments for the object

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md): Opens the pg_description relation for modification
  - [systable_beginscan](../s/systable_beginscan.md): Initiates indexed scan for comments to delete
  - [systable_getnext](../s/systable_getnext.md): Iterates through matching comment tuples
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md): Removes each matching comment tuple
  - [systable_endscan](../s/systable_endscan.md): Ends the systematic scan
  - [table_close](../t/table_close.md): Closes the pg_description relation
- Called from (representative examples):
  - [deleteOneObject](../d/deleteOneObject.md): Dependency system calls during object deletion

## Notes and Other Information
- Uses conditional scan key construction: 2 keys for object-wide deletion, 3 keys for sub-object deletion
- Efficiently uses DescriptionObjIndexId for indexed lookups by (objoid, classoid, objsubid)
- Acquires RowExclusiveLock on pg_description during both open and close operations
- No memory management complexity since it only deletes existing tuples
- Typically invoked by the dependency tracking system during CASCADE deletions

## Simplified Source

```c
void DeleteComments(Oid oid, Oid classoid, int32 subid) {
    Relation description;
    ScanKeyData skey[3];
    int nkeys;
    SysScanDesc sd;
    HeapTuple oldtuple;

    // Set up scan keys for object and class OID
    ScanKeyInit(&skey[0], Anum_pg_description_objoid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oid));
    ScanKeyInit(&skey[1], Anum_pg_description_classoid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classoid));

    // If subid specified, add it as a third scan key
    if (subid != 0) {
        ScanKeyInit(&skey[2], Anum_pg_description_objsubid,
                    BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(subid));
        nkeys = 3;  // Search for specific sub-object
    } else {
        nkeys = 2;  // Search for all sub-objects
    }

    // Open pg_description catalog table
    description = table_open(DescriptionRelationId, RowExclusiveLock);

    // Begin indexed scan using object index
    sd = systable_beginscan(description, DescriptionObjIndexId, true,
                           NULL, nkeys, skey);

    // Delete all matching comment tuples
    while ((oldtuple = systable_getnext(sd)) != NULL) {
        CatalogTupleDelete(description, &oldtuple->t_self);
    }

    // Clean up scan and close table
    systable_endscan(sd);
    table_close(description, RowExclusiveLock);
}
```