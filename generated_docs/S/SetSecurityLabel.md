# SetSecurityLabel

## Location
[src/backend/commands/seclabel.c:404-490](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/seclabel.c#L404-L490)

## Overview
SetSecurityLabel sets or deletes a security label for a specified database object with a given security provider.

## Definition

```c
void
SetSecurityLabel(const ObjectAddress *object,
				 const char *provider, const char *label)
```
## Detailed Description
SetSecurityLabel attempts to set the security label for the specified provider on the specified object to the given value. If the label parameter is NULL, any existing label is deleted. The function handles both regular objects (stored in pg_seclabel) and shared objects (which have their own security label catalog and are handled via SetSharedSecurityLabel).

The function performs the following operations:
1. Checks if the object is a shared relation and delegates to SetSharedSecurityLabel if so
2. Searches for an existing security label entry using a system catalog scan
3. If an existing entry is found:
   - Deletes it if the new label is NULL
   - Updates it with the new label value if the label is not NULL
4. If no existing entry is found and a label is provided, inserts a new tuple
5. Properly maintains catalog indexes and cleans up memory

## Parameters / Member Variables
- `*object`: Pointer to ObjectAddress structure identifying the target database object (contains classId, objectId, and objectSubId)
- `*provider`: String identifying the security label provider (e.g., 'selinux')
- `*label`: The security label string to set, or NULL to delete any existing label
## Dependencies
- Functions called/Symbols referenced:
  - [IsSharedRelation](../I/IsSharedRelation.md)
  - [SetSharedSecurityLabel](SetSharedSecurityLabel.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [CatalogTupleDelete](../C/CatalogTupleDelete.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [ExecSecLabelStmt](../E/ExecSecLabelStmt.md)

## Notes and Other Information
- The function distinguishes between shared and non-shared objects, routing shared objects to SetSharedSecurityLabel
- Uses the SecLabelObjectIndexId index for efficient searching of existing labels
- Properly handles memory management by freeing heap tuples when done
- Maintains transactional consistency by using RowExclusiveLock on the pg_seclabel relation
- The function is the primary entry point for the SECURITY LABEL SQL command execution

## Simplified Source

```c
void
SetSecurityLabel(const ObjectAddress *object,
                 const char *provider, const char *label)
{
    // Handle shared objects separately
    if (IsSharedRelation(object->classId)) {
        SetSharedSecurityLabel(object, provider, label);
        return;
    }

    // Open security label catalog
    Relation pg_seclabel = table_open(SecLabelRelationId, RowExclusiveLock);

    // Search for existing label entry
    ScanKeyData keys[4];
    ScanKeyInit(&keys[0], Anum_pg_seclabel_objoid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(object->objectId));
    ScanKeyInit(&keys[1], Anum_pg_seclabel_classoid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(object->classId));
    ScanKeyInit(&keys[2], Anum_pg_seclabel_objsubid, BTEqualStrategyNumber,
                F_INT4EQ, Int32GetDatum(object->objectSubId));
    ScanKeyInit(&keys[3], Anum_pg_seclabel_provider, BTEqualStrategyNumber,
                F_TEXTEQ, CStringGetTextDatum(provider));

    SysScanDesc scan = systable_beginscan(pg_seclabel, SecLabelObjectIndexId,
                                          true, NULL, 4, keys);
    HeapTuple oldtup = systable_getnext(scan);

    if (HeapTupleIsValid(oldtup)) {
        if (label == NULL) {
            // Delete existing label
            CatalogTupleDelete(pg_seclabel, &oldtup->t_self);
        } else {
            // Update existing label
            Datum values[Natts_pg_seclabel];
            bool nulls[Natts_pg_seclabel];
            bool replaces[Natts_pg_seclabel];

            memset(nulls, false, sizeof(nulls));
            memset(replaces, false, sizeof(replaces));
            replaces[Anum_pg_seclabel_label - 1] = true;
            values[Anum_pg_seclabel_label - 1] = CStringGetTextDatum(label);

            HeapTuple newtup = heap_modify_tuple(oldtup, RelationGetDescr(pg_seclabel),
                                                 values, nulls, replaces);
            CatalogTupleUpdate(pg_seclabel, &oldtup->t_self, newtup);
            heap_freetuple(newtup);
        }
    } else if (label != NULL) {
        // Insert new label entry
        Datum values[Natts_pg_seclabel];
        bool nulls[Natts_pg_seclabel];

        memset(nulls, false, sizeof(nulls));
        values[Anum_pg_seclabel_objoid - 1] = ObjectIdGetDatum(object->objectId);
        values[Anum_pg_seclabel_classoid - 1] = ObjectIdGetDatum(object->classId);
        values[Anum_pg_seclabel_objsubid - 1] = Int32GetDatum(object->objectSubId);
        values[Anum_pg_seclabel_provider - 1] = CStringGetTextDatum(provider);
        values[Anum_pg_seclabel_label - 1] = CStringGetTextDatum(label);

        HeapTuple newtup = heap_form_tuple(RelationGetDescr(pg_seclabel), values, nulls);
        CatalogTupleInsert(pg_seclabel, newtup);
        heap_freetuple(newtup);
    }

    systable_endscan(scan);
    table_close(pg_seclabel, RowExclusiveLock);
}
```