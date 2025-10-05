# RememberAllDependentForRebuilding

## Location
[src/backend/commands/tablecmds.c:13463-13688](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L13463-L13688)

## Overview
RememberAllDependentForRebuilding scans for all objects that depend on a specific column and records information necessary to recreate those objects after a column type change or expression modification.

## Definition

```c
static void
RememberAllDependentForRebuilding(AlteredTableInfo *tab, AlterTableType subtype,
								  Relation rel, AttrNumber attnum, const char *colName)
```
## Detailed Description
This function performs a comprehensive dependency analysis for a specific column by scanning the pg_depend system catalog. It identifies all objects that reference the column and categorizes them for appropriate handling:

1. **Index Dependencies**: Records indexes for rebuilding via RememberIndexForRebuilding
2. **Constraint Dependencies**: Records constraints for rebuilding via RememberConstraintForRebuilding  
3. **Statistics Dependencies**: Records extended statistics for rebuilding via RememberStatisticsForRebuilding
4. **Sequence Dependencies**: Handles SERIAL column sequences (no action needed)
5. **Generated Column Dependencies**: Prevents type changes when column is used by generated columns
6. **Restrictive Dependencies**: Blocks type changes for objects that cannot be automatically updated:
   - Functions/procedures that reference the column
   - Views/rules that reference the column  
   - Triggers with WHEN conditions using the column
   - RLS policies using the column
   - Publication WHERE clauses using the column

The function differentiates between AT_AlterColumnType and AT_SetExpression operations, with stricter restrictions for type changes than expression changes.

## Parameters / Member Variables
- `*tab`: AlteredTableInfo structure to store rebuilding information
- `subtype`: AlterTableType indicating the operation (AT_AlterColumnType or AT_SetExpression)
- `rel`: Relation containing the column being modified
- `attnum`: Attribute number of the column being modified
- `*colName`: Name of the column being modified (for error messages)
## Dependencies
- Functions called/Symbols referenced:
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [get_rel_relkind](../g/get_rel_relkind.md)
  - [RememberIndexForRebuilding](RememberIndexForRebuilding.md)
  - [RememberConstraintForRebuilding](RememberConstraintForRebuilding.md)
  - [RememberStatisticsForRebuilding](RememberStatisticsForRebuilding.md)
  - [GetAttrDefaultColumnAddress](../G/GetAttrDefaultColumnAddress.md)
  - [getObjectDescription](../g/getObjectDescription.md)
  - [get_attname](../g/get_attname.md)
- Called from (representative examples):
  - [ATExecAlterColumnType](../A/ATExecAlterColumnType.md)
  - [ATExecSetExpression](../A/ATExecSetExpression.md)

## Notes and Other Information
- Uses DependReferenceIndexId for efficient dependency scanning
- Prevents potentially unsafe operations by blocking type changes for complex dependencies
- Handles both direct and indirect column dependencies
- Provides detailed error messages with object descriptions when blocking operations
- FIXME comments indicate areas where future improvements could enable currently blocked operations

## Simplified Source

```c
static void RememberAllDependentForRebuilding(AlteredTableInfo *tab, AlterTableType subtype,
                                             Relation rel, AttrNumber attnum, const char *colName) {
    Relation depRel;
    ScanKeyData key[3];
    SysScanDesc scan;
    HeapTuple depTup;

    Assert(subtype == AT_AlterColumnType || subtype == AT_SetExpression);

    // Open pg_depend catalog to find dependencies
    depRel = table_open(DependRelationId, RowExclusiveLock);

    // Set up scan keys to find objects that depend on this column
    ScanKeyInit(&key[0], Anum_pg_depend_refclassid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationRelationId));
    ScanKeyInit(&key[1], Anum_pg_depend_refobjid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationGetRelid(rel)));
    ScanKeyInit(&key[2], Anum_pg_depend_refobjsubid, BTEqualStrategyNumber, F_INT4EQ,
                Int32GetDatum((int32) attnum));

    scan = systable_beginscan(depRel, DependReferenceIndexId, true, NULL, 3, key);

    // Process each dependent object
    while (HeapTupleIsValid(depTup = systable_getnext(scan))) {
        Form_pg_depend foundDep = (Form_pg_depend) GETSTRUCT(depTup);
        ObjectAddress foundObject;

        foundObject.classId = foundDep->classid;
        foundObject.objectId = foundDep->objid;
        foundObject.objectSubId = foundDep->objsubid;

        switch (foundObject.classId) {
            case RelationRelationId:
                {
                    char relKind = get_rel_relkind(foundObject.objectId);

                    if (relKind == RELKIND_INDEX || relKind == RELKIND_PARTITIONED_INDEX) {
                        // Remember index for rebuilding
                        RememberIndexForRebuilding(foundObject.objectId, tab);
                    } else if (relKind == RELKIND_SEQUENCE) {
                        // SERIAL sequence - no action needed
                    } else {
                        elog(ERROR, "unexpected object depending on column: %s",
                             getObjectDescription(&foundObject, false));
                    }
                    break;
                }

            case ConstraintRelationId:
                // Remember constraint for rebuilding
                RememberConstraintForRebuilding(foundObject.objectId, tab);
                break;

            case StatisticExtRelationId:
                // Remember extended statistics for rebuilding
                RememberStatisticsForRebuilding(foundObject.objectId, tab);
                break;

            case AttrDefaultRelationId:
                {
                    ObjectAddress col = GetAttrDefaultColumnAddress(foundObject.objectId);

                    if (col.objectId == RelationGetRelid(rel) && col.objectSubId == attnum) {
                        // This is the column's own default - caller handles it
                    } else {
                        // Generated column dependency - block type changes
                        if (subtype == AT_AlterColumnType)
                            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                           errmsg("cannot alter type of a column used by a generated column")));
                    }
                    break;
                }

            case ProcedureRelationId:
            case RewriteRelationId:
            case TriggerRelationId:
            case PolicyRelationId:
            case PublicationRelRelationId:
                // These dependencies block type changes
                if (subtype == AT_AlterColumnType)
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                   errmsg("cannot alter type of a column used by %s",
                                          getObjectDescription(&foundObject, false))));
                break;

            default:
                elog(ERROR, "unexpected object depending on column: %s",
                     getObjectDescription(&foundObject, false));
                break;
        }
    }

    systable_endscan(scan);
    table_close(depRel, NoLock);
}
```