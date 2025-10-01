# ATExecAddOf

## Location
[src/backend/commands/tablecmds.c:16486-16627](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L16486-L16627)

## Overview
Executes the ALTER TABLE OF command to attach a table to a composite type, making it a typed table with structure matching the specified type.

## Definition

```c
static ObjectAddress
ATExecAddOf(Relation rel, const TypeName *ofTypename, LOCKMODE lockmode)
```
## Detailed Description
ATExecAddOf implements the ALTER TABLE OF SQL command that converts a regular table into a typed table by associating it with a composite type. The function performs comprehensive validation to ensure the table structure exactly matches the type definition, including column names, data types, type modifiers, and collations in the same order. It enforces that typed tables cannot have inheritance relationships and ensures the table structure is compatible with what could have been created using CREATE TABLE OF. If the table was previously typed, it removes the old type dependency before establishing the new one.

## Parameters / Member Variables
- : The relation to be converted to a typed table
- : TypeName structure identifying the composite type to attach to the table
- : Lock mode parameter for the operation

## Dependencies
- Functions called/Symbols referenced:
  - [typenameType](../t/typenameType.md)
  - [check_of_type](../c/check_of_type.md)
  - [table_open](../t/table_open.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [lookup_rowtype_tupdesc](../l/lookup_rowtype_tupdesc.md)
  - TupleDescAttr
  - ReleaseTupleDesc
  - [drop_parent_dependency](../d/drop_parent_dependency.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - SearchSysCacheCopy1
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md)

## Notes and Other Information
- Validates that the table has no inheritance relationships, preventing typed tables from participating in inheritance
- Performs strict compatibility checking between table and type structure, requiring exact matches for column names, types, type modifiers, and collations
- Handles the case where a table is already typed by removing the previous type dependency before establishing the new one
- Updates pg_class.reloftype to record the type association
- Uses DEPENDENCY_NORMAL for the relationship between table and type
- Ensures that any extra columns beyond those in the type definition must be dropped columns
- Returns an ObjectAddress representing the composite type that was attached to the table
- Invokes post-alter hooks to notify other subsystems of the table modification

## Simplified Source

```c
static ObjectAddress
ATExecAddOf(Relation rel, const TypeName *ofTypename, LOCKMODE lockmode)
{
    Oid relid = RelationGetRelid(rel);
    Type typetuple;
    Oid typeid;
    TupleDesc typeTupleDesc, tableTupleDesc;
    ObjectAddress tableobj, typeobj;
    HeapTuple classtuple;

    // Validate the composite type
    typetuple = typenameType(NULL, ofTypename, NULL);
    check_of_type(typetuple);
    typeid = ((Form_pg_type) GETSTRUCT(typetuple))->oid;

    // Check that table has no inheritance parents (typed tables cannot inherit)
    Relation inheritsRelation = table_open(InheritsRelationId, AccessShareLock);
    ScanKeyData key;
    ScanKeyInit(&key, Anum_pg_inherits_inhrelid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(relid));
    SysScanDesc scan = systable_beginscan(inheritsRelation, InheritsRelidSeqnoIndexId,
                                         true, NULL, 1, &key);
    if (HeapTupleIsValid(systable_getnext(scan)))
        ereport(ERROR, "typed tables cannot inherit");
    systable_endscan(scan);
    table_close(inheritsRelation, AccessShareLock);

    // Compare table and type structure for exact compatibility
    typeTupleDesc = lookup_rowtype_tupdesc(typeid, -1);
    tableTupleDesc = RelationGetDescr(rel);

    AttrNumber table_attno = 1;
    for (AttrNumber type_attno = 1; type_attno <= typeTupleDesc->natts; type_attno++) {
        // Get next non-dropped type attribute
        Form_pg_attribute type_attr = TupleDescAttr(typeTupleDesc, type_attno - 1);
        if (type_attr->attisdropped)
            continue;

        // Get next non-dropped table attribute
        Form_pg_attribute table_attr;
        do {
            if (table_attno > tableTupleDesc->natts)
                ereport(ERROR, "table is missing column \"%s\"", NameStr(type_attr->attname));
            table_attr = TupleDescAttr(tableTupleDesc, table_attno - 1);
            table_attno++;
        } while (table_attr->attisdropped);

        // Verify exact match: name, type, typmod, collation
        if (strncmp(NameStr(table_attr->attname), NameStr(type_attr->attname), NAMEDATALEN) != 0)
            ereport(ERROR, "column name mismatch");

        if (table_attr->atttypid != type_attr->atttypid ||
            table_attr->atttypmod != type_attr->atttypmod ||
            table_attr->attcollation != type_attr->attcollation)
            ereport(ERROR, "table has different type for column");
    }
    ReleaseTupleDesc(typeTupleDesc);

    // Ensure any remaining table columns are dropped
    for (; table_attno <= tableTupleDesc->natts; table_attno++) {
        Form_pg_attribute table_attr = TupleDescAttr(tableTupleDesc, table_attno - 1);
        if (!table_attr->attisdropped)
            ereport(ERROR, "table has extra column \"%s\"", NameStr(table_attr->attname));
    }

    // Remove old type dependency if table was previously typed
    if (rel->rd_rel->reloftype)
        drop_parent_dependency(relid, TypeRelationId, rel->rd_rel->reloftype, DEPENDENCY_NORMAL);

    // Record dependency on new type
    ObjectAddressSet(tableobj, RelationRelationId, relid);
    ObjectAddressSet(typeobj, TypeRelationId, typeid);
    recordDependencyOn(&tableobj, &typeobj, DEPENDENCY_NORMAL);

    // Update pg_class.reloftype to record the type association
    Relation relationRelation = table_open(RelationRelationId, RowExclusiveLock);
    classtuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
    ((Form_pg_class) GETSTRUCT(classtuple))->reloftype = typeid;
    CatalogTupleUpdate(relationRelation, &classtuple->t_self, classtuple);

    InvokeObjectPostAlterHook(RelationRelationId, relid, 0);
    heap_freetuple(classtuple);
    table_close(relationRelation, RowExclusiveLock);
    ReleaseSysCache(typetuple);

    return typeobj;
}
```