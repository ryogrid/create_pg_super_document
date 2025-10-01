# ATExecDropExpression

## Location
[src/backend/commands/tablecmds.c:8519-8609](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L8519-L8609)

## Overview
ATExecDropExpression executes the DROP EXPRESSION command for ALTER TABLE operations, removing the generated expression from a stored generated column and returning the address of the affected column.

## Definition
static ObjectAddress ATExecDropExpression(Relation rel, const char *colName, bool missing_ok, LOCKMODE lockmode)

## Detailed Description
This function performs the actual execution of dropping a generated expression from a stored generated column. It validates that the specified column exists and is indeed a stored generated column, then proceeds to remove the generation expression by clearing the attgenerated flag, dropping dependency records, and removing the default expression. The function handles both error and graceful failure cases based on the missing_ok parameter.

The operation involves multiple steps: validating the column, updating the pg_attribute catalog to mark the column as no longer generated, cleaning up dependency records, and finally removing the actual default expression that contained the generation logic.

## Parameters / Member Variables
- : The relation (table) containing the column to be modified
- : The name of the column from which to drop the generated expression
- : Boolean flag indicating whether to issue a notice instead of an error if the column is not a generated column
- : The lock mode to use for accessing related catalog tables

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCacheCopyAttName](../S/SearchSysCacheCopyAttName.md)
  - ATTRIBUTE_GENERATED_STORED
  - [heap_freetuple](../h/heap_freetuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
  - [GetAttrDefaultOid](../G/GetAttrDefaultOid.md)
  - [deleteDependencyRecordsFor](../d/deleteDependencyRecordsFor.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
  - [RemoveAttrDefault](../R/RemoveAttrDefault.md)
  - ObjectAddressSubSet
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md)

## Notes and Other Information
- The function returns InvalidObjectAddress if the operation is skipped due to missing_ok being true
- System columns cannot have their generated expressions dropped
- The function ensures proper cleanup of both the pg_attribute entry and the pg_attrdef dependency records
- Uses RESTRICT mode when removing the attribute default for safety
- Invokes post-alter hooks to notify other subsystems of the change

## Simplified Source

```c
static ObjectAddress
ATExecDropExpression(Relation rel, const char *colName, bool missing_ok, LOCKMODE lockmode)
{
    HeapTuple tuple;
    Form_pg_attribute attTup;
    AttrNumber attnum;
    Relation attrelation;
    Oid attrdefoid;
    ObjectAddress address;

    // Open attribute catalog for updates
    attrelation = table_open(AttributeRelationId, RowExclusiveLock);

    // Find the target column
    tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), colName);
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errmsg("column \"%s\" of relation \"%s\" does not exist",
                               colName, RelationGetRelationName(rel))));

    attTup = (Form_pg_attribute) GETSTRUCT(tuple);
    attnum = attTup->attnum;

    // Validate column type
    if (attnum <= 0)
        ereport(ERROR, (errmsg("cannot alter system column \"%s\"", colName)));

    // Check if column is a stored generated column
    if (attTup->attgenerated != ATTRIBUTE_GENERATED_STORED)
    {
        if (missing_ok)
        {
            ereport(NOTICE, (errmsg("column \"%s\" is not a stored generated column, skipping", colName)));
            heap_freetuple(tuple);
            table_close(attrelation, RowExclusiveLock);
            return InvalidObjectAddress;
        }
        ereport(ERROR, (errmsg("column \"%s\" is not a stored generated column", colName)));
    }

    // Mark column as no longer generated
    attTup->attgenerated = '\0';
    CatalogTupleUpdate(attrelation, &tuple->t_self, tuple);

    // Notify other subsystems of the change
    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum);
    heap_freetuple(tuple);
    table_close(attrelation, RowExclusiveLock);

    // Remove dependency records for the generated expression
    attrdefoid = GetAttrDefaultOid(RelationGetRelid(rel), attnum);
    if (!OidIsValid(attrdefoid))
        elog(ERROR, "could not find attrdef tuple for relation %u attnum %d",
             RelationGetRelid(rel), attnum);

    deleteDependencyRecordsFor(AttrDefaultRelationId, attrdefoid, false);
    CommandCounterIncrement();

    // Remove the generated expression itself
    RemoveAttrDefault(RelationGetRelid(rel), attnum, DROP_RESTRICT, false, false);

    // Return address of the modified column
    ObjectAddressSubSet(address, RelationRelationId, RelationGetRelid(rel), attnum);
    return address;
}
```