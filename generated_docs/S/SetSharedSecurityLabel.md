# SetSharedSecurityLabel

## Location
src/backend/commands/seclabel.c: 329 - 403

## Overview
Sets or removes a security label for a shared database object in the pg_shseclabel system catalog, handling insert, update, and delete operations as needed.

## Definition
```c
static void SetSharedSecurityLabel(const ObjectAddress *object, const char *provider, const char *label)
```

## Detailed Description
This function manages security labels for shared database objects (cluster-wide objects) by performing CRUD operations on the pg_shseclabel system catalog. It implements a complete upsert mechanism that handles three scenarios:

1. **Label Creation**: When no existing label exists and a new label is provided, it creates a new tuple and inserts it into the catalog.

2. **Label Update**: When an existing label is found and a new label is provided, it modifies the existing tuple with the new label value.

3. **Label Deletion**: When an existing label is found and the label parameter is NULL, it deletes the existing tuple from the catalog.

The function uses a three-key scan (objectId, classId, provider) to locate existing entries, then performs the appropriate catalog operation. It acquires a RowExclusiveLock on the pg_shseclabel catalog to ensure exclusive access during modifications and maintains proper tuple memory management by freeing allocated tuples when operations complete.

## Parameters / Member Variables
- `object`: A pointer to an ObjectAddress structure containing the objectId and classId of the target shared object
- `provider`: A C string specifying the name of the security label provider
- `label`: A C string containing the new security label text, or NULL to remove an existing label

## Dependencies
- Functions called/Symbols referenced:
  - ScanKeyInit
  - table_open
  - systable_beginscan
  - systable_getnext
  - CatalogTupleDelete
  - heap_modify_tuple
  - CatalogTupleUpdate
  - heap_form_tuple
  - CatalogTupleInsert
  - heap_freetuple
  - systable_endscan
  - table_close
  - ObjectIdGetDatum
  - CStringGetTextDatum
  - RelationGetDescr
  - HeapTupleIsValid
- Called from (representative examples):
  - SetSecurityLabel

## Notes and Other Information
- This is a static helper function specifically designed for shared objects, complementing the main SetSecurityLabel function
- Uses RowExclusiveLock on the pg_shseclabel catalog to prevent concurrent modifications during the operation
- Implements proper transaction semantics - changes are part of the current transaction and will be committed or rolled back accordingly
- Handles the case where label is NULL by deleting existing entries, enabling label removal functionality
- Uses the SharedSecLabelObjectIndexId index for efficient catalog scanning during the search phase
- Properly manages heap tuple memory by freeing any newly created tuples after catalog operations
- The function performs catalog operations through the standard PostgreSQL catalog interface (CatalogTupleInsert, CatalogTupleUpdate, CatalogTupleDelete)
- Unlike regular objects, shared objects don't have a sub-object ID component, so only three keys are used in the scan