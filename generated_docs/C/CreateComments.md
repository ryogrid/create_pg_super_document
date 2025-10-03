# CreateComments

## Location
[src/backend/commands/comment.c:143-237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/comment.c#L143-L237)

## Overview
Creates, updates, or deletes comments for database objects by manipulating the pg_description system catalog table.

## Definition

```c
void
CreateComments(Oid oid, Oid classoid, int32 subid, const char *comment)
```
## Detailed Description
CreateComments manages object comments in the pg_description catalog table. It performs insert, update, or delete operations based on the comment parameter: inserts new comments, updates existing ones, or deletes entries when the comment is NULL or empty. The function uses a systematic scan with composite keys (object OID, class OID, subobject ID) to locate existing entries and handles all tuple operations through the catalog interface functions.

The function treats empty strings as NULL comments, effectively deleting any existing comment. It uses heap tuple operations and the catalog update functions to maintain consistency with PostgreSQL's MVCC model.

## Parameters / Member Variables
- `oid`: Object identifier of the target database object
- `classoid`: OID of the system catalog containing the object (e.g., RelationRelationId for tables)
- `subid`: Sub-object identifier (e.g., column number for column comments, 0 for object-level comments)
- `*comment`: Comment text to store, or NULL to delete existing comment
## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md): Opens the pg_description relation for modification
  - [systable_beginscan](../s/systable_beginscan.md): Initiates indexed scan for existing comment
  - [systable_getnext](../s/systable_getnext.md): Retrieves matching tuples from the scan
  - [CatalogTupleDelete](CatalogTupleDelete.md): Removes existing comment tuple
  - [heap_modify_tuple](../h/heap_modify_tuple.md): Creates updated tuple with new comment
  - [CatalogTupleUpdate](CatalogTupleUpdate.md): Updates existing tuple in catalog
  - [heap_form_tuple](../h/heap_form_tuple.md): Creates new tuple for insertion
  - [CatalogTupleInsert](CatalogTupleInsert.md): Inserts new comment tuple
  - [heap_freetuple](../h/heap_freetuple.md): Frees allocated tuple memory
- Called from (representative examples):
  - [CommentObject](CommentObject.md): Main COMMENT ON command handler
  - [DefineIndex](../D/DefineIndex.md): Adds comments during index creation
  - [CreateStatistics](CreateStatistics.md): Adds comments during statistics object creation
  - [CreateExtensionInternal](CreateExtensionInternal.md): Handles extension object comments

## Notes and Other Information
- Uses DescriptionObjIndexId for efficient lookups by (objoid, classoid, objsubid)
- Assumes only one matching tuple exists per key combination
- Empty strings are normalized to NULL for consistent behavior
- Acquires RowExclusiveLock on pg_description to prevent concurrent modifications
- Memory management includes proper cleanup of temporary heap tuples

## Simplified Source

```c
void CreateComments(Oid oid, Oid classoid, int32 subid, const char *comment) {
    // Normalize empty strings to NULL
    if (comment != NULL && strlen(comment) == 0)
        comment = NULL;

    // Prepare tuple data for insert/update operations
    Datum values[Natts_pg_description];
    bool nulls[Natts_pg_description];
    bool replaces[Natts_pg_description];

    if (comment != NULL) {
        // Set up values for new/updated comment
        for (int i = 0; i < Natts_pg_description; i++) {
            nulls[i] = false;
            replaces[i] = true;
        }
        values[Anum_pg_description_objoid - 1] = ObjectIdGetDatum(oid);
        values[Anum_pg_description_classoid - 1] = ObjectIdGetDatum(classoid);
        values[Anum_pg_description_objsubid - 1] = Int32GetDatum(subid);
        values[Anum_pg_description_description - 1] = CStringGetTextDatum(comment);
    }

    // Search for existing comment using composite key
    ScanKeyData skey[3];
    ScanKeyInit(&skey[0], Anum_pg_description_objoid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oid));
    ScanKeyInit(&skey[1], Anum_pg_description_classoid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classoid));
    ScanKeyInit(&skey[2], Anum_pg_description_objsubid, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(subid));

    Relation description = table_open(DescriptionRelationId, RowExclusiveLock);
    SysScanDesc scan = systable_beginscan(description, DescriptionObjIndexId, true, NULL, 3, skey);

    HeapTuple oldtuple;
    HeapTuple newtuple = NULL;

    // Process existing tuple if found
    if ((oldtuple = systable_getnext(scan)) != NULL) {
        if (comment == NULL) {
            // Delete existing comment
            CatalogTupleDelete(description, &oldtuple->t_self);
        } else {
            // Update existing comment
            newtuple = heap_modify_tuple(oldtuple, RelationGetDescr(description), values, nulls, replaces);
            CatalogTupleUpdate(description, &oldtuple->t_self, newtuple);
        }
    }

    systable_endscan(scan);

    // Insert new comment if no existing tuple was found
    if (newtuple == NULL && comment != NULL) {
        newtuple = heap_form_tuple(RelationGetDescr(description), values, nulls);
        CatalogTupleInsert(description, newtuple);
    }

    // Cleanup
    if (newtuple != NULL)
        heap_freetuple(newtuple);
    table_close(description, NoLock);
}
```