# CreateSharedComments

## Location
[src/backend/commands/comment.c:238-325](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/comment.c#L238-L325)

## Overview
Creates, updates, or deletes comments for cluster-wide shared objects by manipulating the pg_shdescription system catalog table.

## Definition

```c
void
CreateSharedComments(Oid oid, Oid classoid, const char *comment)
```
## Detailed Description
CreateSharedComments manages comments for cluster-wide shared objects (databases, tablespaces, roles) in the pg_shdescription catalog table. It operates similarly to CreateComments but works with shared objects that exist across the entire database cluster rather than within a single database. The function performs insert, update, or delete operations based on the comment parameter, using a two-key lookup system (object OID and class OID) since shared objects don't have sub-objects.

Like CreateComments, it treats empty strings as NULL comments for deletion, and uses the standard catalog interface functions to maintain MVCC consistency.

## Parameters / Member Variables
- : Object identifier of the target shared object (database, tablespace, or role OID)
- : OID of the system catalog containing the shared object (e.g., DatabaseRelationId, TableSpaceRelationId, AuthIdRelationId)
- : Comment text to store, or NULL to delete existing comment

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md): Opens the pg_shdescription relation for modification
  - [systable_beginscan](../s/systable_beginscan.md): Initiates indexed scan using SharedDescriptionObjIndexId
  - [systable_getnext](../s/systable_getnext.md): Retrieves matching tuples from the scan
  - [CatalogTupleDelete](CatalogTupleDelete.md): Removes existing comment tuple
  - [heap_modify_tuple](../h/heap_modify_tuple.md): Creates updated tuple with new comment
  - [CatalogTupleUpdate](CatalogTupleUpdate.md): Updates existing tuple in catalog
  - [heap_form_tuple](../h/heap_form_tuple.md): Creates new tuple for insertion
  - [CatalogTupleInsert](CatalogTupleInsert.md): Inserts new comment tuple
  - [heap_freetuple](../h/heap_freetuple.md): Frees allocated tuple memory
- Called from (representative examples):
  - [CommentObject](CommentObject.md): Routes shared object comments to this function

## Notes and Other Information
- Uses SharedDescriptionObjIndexId for efficient lookups by (objoid, classoid)
- Only handles cluster-wide objects: databases, tablespaces, and roles
- Does not use sub-object IDs since shared objects don't have sub-components
- Acquires RowExclusiveLock on pg_shdescription to prevent concurrent modifications
- Follows same empty-string-to-NULL normalization as CreateComments
- Memory management includes proper cleanup of temporary heap tuples

## Simplified Source

```c
void
CreateSharedComments(Oid oid, Oid classoid, const char *comment)
{
    Relation shdescription;
    ScanKeyData skey[2];
    SysScanDesc sd;
    HeapTuple oldtuple;
    HeapTuple newtuple = NULL;
    Datum values[Natts_pg_shdescription];
    bool nulls[Natts_pg_shdescription];
    bool replaces[Natts_pg_shdescription];

    // Normalize empty string to NULL
    if (comment != NULL && strlen(comment) == 0)
        comment = NULL;

    // Prepare tuple data if we have a comment to insert/update
    if (comment != NULL) {
        for (int i = 0; i < Natts_pg_shdescription; i++) {
            nulls[i] = false;
            replaces[i] = true;
        }
        values[Anum_pg_shdescription_objoid - 1] = ObjectIdGetDatum(oid);
        values[Anum_pg_shdescription_classoid - 1] = ObjectIdGetDatum(classoid);
        values[Anum_pg_shdescription_description - 1] = CStringGetTextDatum(comment);
    }

    // Set up search keys for (objoid, classoid) lookup
    ScanKeyInit(&skey[0], Anum_pg_shdescription_objoid,
               BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oid));
    ScanKeyInit(&skey[1], Anum_pg_shdescription_classoid,
               BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classoid));

    // Open catalog and search for existing comment
    shdescription = table_open(SharedDescriptionRelationId, RowExclusiveLock);
    sd = systable_beginscan(shdescription, SharedDescriptionObjIndexId, true,
                           NULL, 2, skey);

    while ((oldtuple = systable_getnext(sd)) != NULL) {
        if (comment == NULL) {
            // Delete existing comment
            CatalogTupleDelete(shdescription, &oldtuple->t_self);
        } else {
            // Update existing comment
            newtuple = heap_modify_tuple(oldtuple, RelationGetDescr(shdescription),
                                        values, nulls, replaces);
            CatalogTupleUpdate(shdescription, &oldtuple->t_self, newtuple);
        }
        break; // Assume only one match possible
    }

    systable_endscan(sd);

    // Insert new comment if none existed
    if (newtuple == NULL && comment != NULL) {
        newtuple = heap_form_tuple(RelationGetDescr(shdescription),
                                  values, nulls);
        CatalogTupleInsert(shdescription, newtuple);
    }

    // Cleanup
    if (newtuple != NULL)
        heap_freetuple(newtuple);

    table_close(shdescription, NoLock);
}
```