# ATExecAlterColumnType

## Location
[src/backend/commands/tablecmds.c:13146-13462](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L13146-L13462)

## Overview
ATExecAlterColumnType executes the ALTER COLUMN .. SET DATA TYPE command, handling the complex process of changing a column's data type while managing dependencies, defaults, and constraints.

## Definition

```c
struct_array(&missingval,
														 1,
														 targettype,
														 tform->typlen,
														 tform->typbyval,
														 tform->typalign));
```
## Detailed Description
This function implements the core logic for changing a column's data type in PostgreSQL tables. It performs comprehensive validation and dependency management:

1. **Missing Value Management**: Clears missing values when table rewriting is required
2. **Column Validation**: Verifies the target column exists and prevents multiple type changes
3. **Type Coercion**: Validates that existing default expressions can be coerced to the new type
4. **Dependency Tracking**: Uses RememberAllDependentForRebuilding to record objects that need rebuilding
5. **Catalog Updates**: Updates pg_attribute with new type information including type OID, typmod, collation, and storage parameters
6. **Default Expression Handling**: Removes and recreates default expressions with proper type coercion
7. **Statistics Cleanup**: Removes obsolete statistics entries for the column

The function ensures data integrity by carefully managing all dependent objects and maintaining referential consistency throughout the type change operation.

## Parameters / Member Variables
- : AlteredTableInfo structure containing table modification context and rewrite information
- : Relation being modified
- : AlterTableCmd containing the column name and new type definition
- : Lock mode for the operation

## Dependencies
- Functions called/Symbols referenced:
  - [RememberAllDependentForRebuilding](../R/RememberAllDependentForRebuilding.md)
  - [SearchSysCacheCopyAttName](../S/SearchSysCacheCopyAttName.md)
  - [typenameType](../t/typenameType.md)
  - [GetColumnDefCollation](../G/GetColumnDefCollation.md)
  - [build_column_default](../b/build_column_default.md)
  - [strip_implicit_coercions](../s/strip_implicit_coercions.md)
  - [coerce_to_target_type](../c/coerce_to_target_type.md)
  - [add_column_datatype_dependency](../a/add_column_datatype_dependency.md)
  - [add_column_collation_dependency](../a/add_column_collation_dependency.md)
  - [RemoveStatistics](../R/RemoveStatistics.md)
  - [GetAttrDefaultOid](../G/GetAttrDefaultOid.md)
  - [deleteDependencyRecordsFor](../d/deleteDependencyRecordsFor.md)
  - [RemoveAttrDefault](../R/RemoveAttrDefault.md)
  - [StoreAttrDefault](../S/StoreAttrDefault.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [relation_close](../r/relation_close.md)
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md) (main ALTER TABLE command dispatcher)

## Notes and Other Information
- Handles both regular and generated column defaults differently
- Manages missing value arrays when changing types without table rewrite
- Prevents multiple ALTER TYPE operations on the same column in one transaction
- Updates compression method to invalid when changing types
- Maintains array dimension information from the type specification
- Uses RESTRICT mode when removing old defaults for safety

## Simplified Source

```c
static ObjectAddress
ATExecAlterColumnType(AlteredTableInfo *tab, Relation rel,
                     AlterTableCmd *cmd, LOCKMODE lockmode)
{
    char *colName = cmd->name;
    ColumnDef *def = (ColumnDef *) cmd->def;
    TypeName *typeName = def->typeName;
    HeapTuple heapTup, typeTuple;
    Form_pg_attribute attTup, attOldTup;
    AttrNumber attnum;
    Oid targettype, targetcollid;
    int32 targettypmod;
    Node *defaultexpr;
    Relation attrelation;
    ObjectAddress address;

    // Clear missing values if rewriting table
    if (tab->rewrite) {
        Relation newrel = table_open(RelationGetRelid(rel), NoLock);
        RelationClearMissing(newrel);
        relation_close(newrel, NoLock);
        CommandCounterIncrement();
    }

    // Look up target column and validate
    attrelation = table_open(AttributeRelationId, RowExclusiveLock);
    heapTup = SearchSysCacheCopyAttName(RelationGetRelid(rel), colName);
    if (!HeapTupleIsValid(heapTup))
        ereport(ERROR, "column \"%s\" does not exist", colName);

    attTup = (Form_pg_attribute) GETSTRUCT(heapTup);
    attnum = attTup->attnum;
    attOldTup = TupleDescAttr(tab->oldDesc, attnum - 1);

    // Prevent multiple ALTER TYPE on same column
    if (attTup->atttypid != attOldTup->atttypid ||
        attTup->atttypmod != attOldTup->atttypmod)
        ereport(ERROR, "cannot alter type of column \"%s\" twice", colName);

    // Look up target type and collation
    typeTuple = typenameType(NULL, typeName, &targettypmod);
    Form_pg_type tform = (Form_pg_type) GETSTRUCT(typeTuple);
    targettype = tform->oid;
    targetcollid = GetColumnDefCollation(NULL, def, targettype);

    // Validate default expression can be coerced to new type
    if (attTup->atthasdef) {
        defaultexpr = build_column_default(rel, attnum);
        defaultexpr = strip_implicit_coercions(defaultexpr);
        defaultexpr = coerce_to_target_type(NULL, defaultexpr, exprType(defaultexpr),
                                          targettype, targettypmod,
                                          COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, -1);
        if (defaultexpr == NULL) {
            if (attTup->attgenerated)
                ereport(ERROR, "generation expression cannot be cast to type %s",
                        format_type_be(targettype));
            else
                ereport(ERROR, "default cannot be cast to type %s",
                        format_type_be(targettype));
        }
    } else {
        defaultexpr = NULL;
    }

    // Record dependencies for rebuilding
    RememberAllDependentForRebuilding(tab, AT_AlterColumnType, rel, attnum, colName);

    // Remove old type/collation dependencies
    Relation depRel = table_open(DependRelationId, RowExclusiveLock);
    ScanKeyData key[3];
    ScanKeyInit(&key[0], Anum_pg_depend_classid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(RelationRelationId));
    ScanKeyInit(&key[1], Anum_pg_depend_objid, BTEqualStrategyNumber,
                F_OIDEQ, ObjectIdGetDatum(RelationGetRelid(rel)));
    ScanKeyInit(&key[2], Anum_pg_depend_objsubid, BTEqualStrategyNumber,
                F_INT4EQ, Int32GetDatum((int32) attnum));

    SysScanDesc scan = systable_beginscan(depRel, DependDependerIndexId, true, NULL, 3, key);
    HeapTuple depTup;
    while (HeapTupleIsValid(depTup = systable_getnext(scan))) {
        Form_pg_depend foundDep = (Form_pg_depend) GETSTRUCT(depTup);
        // Validate it's expected type/collation dependency then delete
        if (foundDep->deptype != DEPENDENCY_NORMAL ||
            (!(foundDep->refclassid == TypeRelationId && foundDep->refobjid == attTup->atttypid) &&
             !(foundDep->refclassid == CollationRelationId && foundDep->refobjid == attTup->attcollation)))
            elog(ERROR, "found unexpected dependency for column");
        CatalogTupleDelete(depRel, &depTup->t_self);
    }
    systable_endscan(scan);
    table_close(depRel, RowExclusiveLock);

    // Handle missing value array update for new type
    if (attTup->atthasmissing && !tab->rewrite) {
        bool isnull;
        Datum missingval = heap_getattr(heapTup, Anum_pg_attribute_attmissingval,
                                       attrelation->rd_att, &isnull);
        if (!isnull) {
            // Rebuild missing value array with new type metadata
            bool isNull;
            int one = 1;
            missingval = array_get_element(missingval, 1, &one, 0,
                                         attTup->attlen, attTup->attbyval, attTup->attalign, &isNull);
            missingval = PointerGetDatum(construct_array(&missingval, 1, targettype,
                                                       tform->typlen, tform->typbyval, tform->typalign));
            // Update tuple with new missing value
            Datum valuesAtt[Natts_pg_attribute] = {0};
            bool nullsAtt[Natts_pg_attribute] = {0};
            bool replacesAtt[Natts_pg_attribute] = {0};
            valuesAtt[Anum_pg_attribute_attmissingval - 1] = missingval;
            replacesAtt[Anum_pg_attribute_attmissingval - 1] = true;
            HeapTuple newTup = heap_modify_tuple(heapTup, RelationGetDescr(attrelation),
                                               valuesAtt, nullsAtt, replacesAtt);
            heap_freetuple(heapTup);
            heapTup = newTup;
            attTup = (Form_pg_attribute) GETSTRUCT(heapTup);
        }
    }

    // Update column type information
    attTup->atttypid = targettype;
    attTup->atttypmod = targettypmod;
    attTup->attcollation = targetcollid;
    attTup->attndims = list_length(typeName->arrayBounds);
    attTup->attlen = tform->typlen;
    attTup->attbyval = tform->typbyval;
    attTup->attalign = tform->typalign;
    attTup->attstorage = tform->typstorage;
    attTup->attcompression = InvalidCompressionMethod;

    // Update catalog and install new dependencies
    ReleaseSysCache(typeTuple);
    CatalogTupleUpdate(attrelation, &heapTup->t_self, heapTup);
    table_close(attrelation, RowExclusiveLock);

    add_column_datatype_dependency(RelationGetRelid(rel), attnum, targettype);
    add_column_collation_dependency(RelationGetRelid(rel), attnum, targetcollid);

    // Remove obsolete statistics
    RemoveStatistics(RelationGetRelid(rel), attnum);
    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum);

    // Update default expression if present
    if (defaultexpr) {
        if (attTup->attgenerated) {
            Oid attrdefoid = GetAttrDefaultOid(RelationGetRelid(rel), attnum);
            (void) deleteDependencyRecordsFor(AttrDefaultRelationId, attrdefoid, false);
        }
        CommandCounterIncrement();
        RemoveAttrDefault(RelationGetRelid(rel), attnum, DROP_RESTRICT, true, true);
        StoreAttrDefault(rel, attnum, defaultexpr, true, false);
    }

    ObjectAddressSubSet(address, RelationRelationId, RelationGetRelid(rel), attnum);
    heap_freetuple(heapTup);
    return address;
}
```