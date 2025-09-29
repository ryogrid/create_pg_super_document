# StoreAttrDefault

## Location
[src/backend/catalog/pg_attrdef.c:46-218](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_attrdef.c#L46-L218)

## Overview
StoreAttrDefault stores a default expression for a specified column in a PostgreSQL relation, creating entries in the pg_attrdef catalog and updating the corresponding pg_attribute entry to mark that a default exists.

## Definition

```c
struct_array(&missingval,
															 1,
															 defAttStruct->atttypid,
															 defAttStruct->attlen,
															 defAttStruct->attbyval,
															 defAttStruct->attalign));
```
## Detailed Description
This function creates a new pg_attrdef tuple to store a default expression for a column. It performs several key operations: converts the expression node to string format for storage, creates a new OID for the default entry, inserts the tuple into the pg_attrdef catalog, updates the pg_attribute entry to set atthasdef to true, and establishes proper dependency relationships. The function also handles special logic for missing values when adding new columns to existing tables, though this code path is currently unused in core PostgreSQL. The function ensures data consistency by acquiring appropriate locks and maintaining referential integrity through the dependency system.

## Parameters / Member Variables
- : The relation (table) containing the column for which the default is being stored
- : The attribute number (column number) within the relation
- : The default expression node to be stored
- : Boolean indicating whether this is an internal default (affects hook invocation)
- : Boolean indicating if this is for a new column (affects missing value handling)

## Dependencies
- Functions called/Symbols referenced:
  - [nodeToString](../n/nodeToString.md): Converts expression node to string representation
  - [GetNewOidWithIndex](../G/GetNewOidWithIndex.md): Generates new OID for the pg_attrdef entry
  - [heap_form_tuple](../h/heap_form_tuple.md): Creates heap tuple from values array
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md): Inserts tuple into catalog
  - [heap_freetuple](../h/heap_freetuple.md): Frees heap tuple memory
  - SearchSysCacheCopy2: Searches system cache for attribute entry
  - [recordDependencyOn](../r/recordDependencyOn.md): Records dependency between default and column
  - [recordDependencyOnSingleRelExpr](../r/recordDependencyOnSingleRelExpr.md): Records dependencies on expression objects
  - InvokeObjectPostCreateHookArg: Invokes post-creation hooks

- Called from (representative examples):
  - [StoreConstraints](StoreConstraints.md): When storing table constraints during creation
  - [AddRelationNewConstraints](../A/AddRelationNewConstraints.md): When adding new constraints to relations
  - [ATExecCookedColumnDefault](../A/ATExecCookedColumnDefault.md): During ALTER TABLE column default operations
  - [ATExecAlterColumnType](../A/ATExecAlterColumnType.md): When changing column types that affect defaults

## Notes and Other Information
The function includes legacy code for handling missing values when adding columns (add_column_mode), but this functionality is currently unused in core PostgreSQL as noted in the comments. The function carefully manages memory allocation and deallocation, freeing temporary structures like the stringified expression and heap tuples. It establishes proper dependency relationships to ensure cascading deletion behavior when columns or tables are dropped. For generated columns, it creates internal dependencies to prevent separate deletion of the default expression.

## Simplified Source

```c
Oid
StoreAttrDefault(Relation rel, AttrNumber attnum, Node *expr,
                 bool is_internal, bool add_column_mode)
{
    // Convert expression to string for storage
    char *adbin = nodeToString(expr);

    // Open pg_attrdef catalog
    Relation adrel = table_open(AttrDefaultRelationId, RowExclusiveLock);

    // Create new pg_attrdef entry
    Oid attrdefOid = GetNewOidWithIndex(adrel, AttrDefaultOidIndexId, Anum_pg_attrdef_oid);

    // Set up tuple values
    Datum values[4];
    bool nulls[4] = {false, false, false, false};
    values[Anum_pg_attrdef_oid - 1] = ObjectIdGetDatum(attrdefOid);
    values[Anum_pg_attrdef_adrelid - 1] = RelationGetRelid(rel);
    values[Anum_pg_attrdef_adnum - 1] = attnum;
    values[Anum_pg_attrdef_adbin - 1] = CStringGetTextDatum(adbin);

    // Insert tuple into pg_attrdef
    HeapTuple tuple = heap_form_tuple(adrel->rd_att, values, nulls);
    CatalogTupleInsert(adrel, tuple);
    table_close(adrel, RowExclusiveLock);

    // Update pg_attribute to mark default exists
    Relation attrrel = table_open(AttributeRelationId, RowExclusiveLock);
    HeapTuple atttup = SearchSysCacheCopy2(ATTNUM,
                                          ObjectIdGetDatum(RelationGetRelid(rel)),
                                          Int16GetDatum(attnum));

    Form_pg_attribute attStruct = (Form_pg_attribute) GETSTRUCT(atttup);
    if (!attStruct->atthasdef)
    {
        // Mark attribute as having default
        Datum valuesAtt[Natts_pg_attribute] = {0};
        bool nullsAtt[Natts_pg_attribute] = {0};
        bool replacesAtt[Natts_pg_attribute] = {0};

        valuesAtt[Anum_pg_attribute_atthasdef - 1] = true;
        replacesAtt[Anum_pg_attribute_atthasdef - 1] = true;

        atttup = heap_modify_tuple(atttup, RelationGetDescr(attrrel),
                                  valuesAtt, nullsAtt, replacesAtt);
        CatalogTupleUpdate(attrrel, &atttup->t_self, atttup);
    }
    table_close(attrrel, RowExclusiveLock);

    // Create dependency relationships
    ObjectAddress defobject, colobject;
    defobject.classId = AttrDefaultRelationId;
    defobject.objectId = attrdefOid;
    defobject.objectSubId = 0;

    colobject.classId = RelationRelationId;
    colobject.objectId = RelationGetRelid(rel);
    colobject.objectSubId = attnum;

    // Record dependencies
    recordDependencyOn(&defobject, &colobject,
                      attStruct->attgenerated ? DEPENDENCY_INTERNAL : DEPENDENCY_AUTO);
    recordDependencyOnSingleRelExpr(&defobject, expr, RelationGetRelid(rel),
                                   DEPENDENCY_NORMAL, DEPENDENCY_NORMAL, false);

    // Cleanup and invoke hooks
    pfree(adbin);
    heap_freetuple(tuple);
    heap_freetuple(atttup);

    InvokeObjectPostCreateHookArg(AttrDefaultRelationId, RelationGetRelid(rel),
                                 attnum, is_internal);

    return attrdefOid;
}
```