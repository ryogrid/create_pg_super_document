# ATExecSetStorage

## Location
[src/backend/commands/tablecmds.c:8887-8949](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L8887-L8949)

## Overview
ATExecSetStorage implements the ALTER TABLE ALTER COLUMN SET STORAGE command, modifying the storage strategy for a specific column in both the table and its associated indexes.

## Definition

```c
structTupleDescriptor()).
	 */
	SetIndexStorageProperties(rel, attrelation, attnum,
							  true, attrtuple->attstorage,
							  false, 0,
							  lockmode);
```
## Detailed Description
This function modifies the storage strategy (PLAIN, EXTERNAL, EXTENDED, or MAIN) for a table column by updating the pg_attribute system catalog. The function validates the column existence, ensures it's not a system column, updates the storage setting in the catalog, and propagates the change to any associated indexes. It also triggers post-alter hooks to notify other subsystems of the change.

The storage strategy determines how PostgreSQL stores variable-length data types, affecting compression and out-of-line storage behavior for TOAST-able columns.

## Parameters / Member Variables
- `rel`: The relation (table) being modified
- `colName`: The name of the column whose storage is being changed
- `newValue`: A Node containing the new storage strategy value (PLAIN, EXTERNAL, EXTENDED, or MAIN)
- `lockmode`: The lock mode to use when accessing related indexes

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCacheCopyAttName](../S/SearchSysCacheCopyAttName.md)
  - [GetAttributeStorage](../G/GetAttributeStorage.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
  - [SetIndexStorageProperties](../S/SetIndexStorageProperties.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - ObjectAddressSubSet
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md)
  - child_dependency_type

## Notes and Other Information
- Located in src/backend/commands/tablecmds.c:8887-8949
- Returns an ObjectAddress pointing to the modified column
- Validates that the column is not a system column (attnum > 0)
- Automatically propagates storage changes to simple index columns
- Uses RowExclusiveLock on the pg_attribute catalog during the update
- The function is static, indicating it's only used within the tablecmds.c module

## Simplified Source

```c
static ObjectAddress
ATExecSetStorage(Relation rel, const char *colName, Node *newValue, LOCKMODE lockmode)
{
    Relation attrelation;
    HeapTuple tuple;
    Form_pg_attribute attrtuple;
    AttrNumber attnum;
    ObjectAddress address;

    // Open the pg_attribute system catalog
    attrelation = table_open(AttributeRelationId, RowExclusiveLock);

    // Find the column by name
    tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), colName);
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                errmsg("column \"%s\" of relation \"%s\" does not exist",
                       colName, RelationGetRelationName(rel))));

    attrtuple = (Form_pg_attribute) GETSTRUCT(tuple);
    attnum = attrtuple->attnum;

    // Validate it's not a system column
    if (attnum <= 0)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("cannot alter system column \"%s\"", colName)));

    // Update the storage strategy in the catalog
    attrtuple->attstorage = GetAttributeStorage(attrtuple->atttypid, strVal(newValue));
    CatalogTupleUpdate(attrelation, &tuple->t_self, tuple);

    // Trigger post-alter hooks
    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attrtuple->attnum);

    // Apply storage change to associated indexes
    SetIndexStorageProperties(rel, attrelation, attnum, true, attrtuple->attstorage,
                             false, 0, lockmode);

    // Cleanup and return column address
    heap_freetuple(tuple);
    table_close(attrelation, RowExclusiveLock);

    ObjectAddressSubSet(address, RelationRelationId, RelationGetRelid(rel), attnum);
    return address;
}
```