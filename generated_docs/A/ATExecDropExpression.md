# ATExecDropExpression

## Location
src/backend/commands/tablecmds.c: 8519 - 8609

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
  - SearchSysCacheCopyAttName
  - ATTRIBUTE_GENERATED_STORED
  - heap_freetuple
  - CatalogTupleUpdate
  - InvokeObjectPostAlterHook
  - GetAttrDefaultOid
  - deleteDependencyRecordsFor
  - CommandCounterIncrement
  - RemoveAttrDefault
  - ObjectAddressSubSet
- Called from (representative examples):
  - ATExecCmd

## Notes and Other Information
- The function returns InvalidObjectAddress if the operation is skipped due to missing_ok being true
- System columns cannot have their generated expressions dropped
- The function ensures proper cleanup of both the pg_attribute entry and the pg_attrdef dependency records
- Uses RESTRICT mode when removing the attribute default for safety
- Invokes post-alter hooks to notify other subsystems of the change