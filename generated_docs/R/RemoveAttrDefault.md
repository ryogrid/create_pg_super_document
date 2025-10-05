# RemoveAttrDefault

## Location
[src/backend/catalog/pg_attrdef.c:219-273](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_attrdef.c#L219-L273)

## Overview
RemoveAttrDefault removes the default expression for a specified column from a relation, deleting the corresponding entry from the pg_attrdef catalog table.

## Definition
```c
void RemoveAttrDefault(Oid relid, AttrNumber attnum, DropBehavior behavior, bool complain, bool internal)
```

## Detailed Description
This function removes an attribute default entry from the pg_attrdef system catalog. It performs a system table scan to locate the default entry for the specified relation and attribute number combination. When found, it uses the object deletion framework (performDeletion) to remove the entry, which properly handles dependency cascading based on the specified drop behavior. The function handles the case where no default exists - it can either raise an error or return silently depending on the complain parameter. The deletion is performed within appropriate locking to ensure consistency.

## Parameters / Member Variables
- `relid`: The OID of the relation containing the column
- `attnum`: The attribute number (column number) whose default should be removed  
- `behavior`: The drop behavior (CASCADE, RESTRICT, etc.) controlling how dependencies are handled
- `complain`: Boolean indicating whether to raise an error if no default is found
- `internal`: Boolean indicating whether this is an internal deletion (affects deletion flags)

## Dependencies
- Functions called/Symbols referenced:
  - [ScanKeyInit](../S/ScanKeyInit.md): Initializes scan keys for system table scanning
  - [systable_beginscan](../s/systable_beginscan.md): Begins systematic scan of system table with index
  - [systable_getnext](../s/systable_getnext.md): Retrieves next tuple from system table scan
  - [systable_endscan](../s/systable_endscan.md): Ends system table scan
  - [performDeletion](../p/performDeletion.md): Performs object deletion with dependency handling
  - [table_open](../t/table_open.md)/table_close: Opens and closes system catalog table
  - PERFORM_DELETION_INTERNAL: Flag for internal deletion operations

- Called from (representative examples):
  - [ATExecColumnDefault](../A/ATExecColumnDefault.md): During ALTER TABLE DROP DEFAULT operations
  - [ATExecCookedColumnDefault](../A/ATExecCookedColumnDefault.md): When processing column default changes
  - [ATExecSetExpression](../A/ATExecSetExpression.md): When setting new expressions (removes old default)
  - [ATExecDropExpression](../A/ATExecDropExpression.md): When dropping column expressions
  - [ATExecAlterColumnType](../A/ATExecAlterColumnType.md): During column type changes that affect defaults

## Notes and Other Information
The function uses a systematic scan of pg_attrdef with the AttrDefaultIndexId index for efficient lookup. Although the comment indicates there should be at most one matching tuple, the implementation uses a loop to handle potential edge cases robustly. The function integrates with PostgreSQL's object deletion framework, ensuring proper dependency cascade handling. When performing internal deletions, special flags are passed to performDeletion to distinguish from user-initiated operations. The function maintains proper locking throughout the operation to ensure catalog consistency.

## Simplified Source

```c
void RemoveAttrDefault(Oid relid, AttrNumber attnum,
                      DropBehavior behavior, bool complain, bool internal) {
    Relation attrdef_rel;
    ScanKeyData scankeys[2];
    SysScanDesc scan;
    HeapTuple tuple;
    bool found = false;

    // Open pg_attrdef catalog for modification
    attrdef_rel = table_open(AttrDefaultRelationId, RowExclusiveLock);

    // Set up scan keys to find the default for this specific column
    ScanKeyInit(&scankeys[0], Anum_pg_attrdef_adrelid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(relid));
    ScanKeyInit(&scankeys[1], Anum_pg_attrdef_adnum, BTEqualStrategyNumber, F_INT2EQ,
                Int16GetDatum(attnum));

    // Scan for matching default entries
    scan = systable_beginscan(attrdef_rel, AttrDefaultIndexId, true, NULL, 2, scankeys);

    // Remove any matching default entries (should be at most one)
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        ObjectAddress object;
        Form_pg_attrdef attrtuple = (Form_pg_attrdef) GETSTRUCT(tuple);

        // Set up object address for deletion
        object.classId = AttrDefaultRelationId;
        object.objectId = attrtuple->oid;
        object.objectSubId = 0;

        // Delete the default using the object deletion framework
        performDeletion(&object, behavior,
                       internal ? PERFORM_DELETION_INTERNAL : 0);

        found = true;
    }

    systable_endscan(scan);
    table_close(attrdef_rel, RowExclusiveLock);

    // Error if no default found and complain is true
    if (complain && !found)
        elog(ERROR, "could not find attrdef tuple for relation %u attnum %d",
             relid, attnum);
}
```