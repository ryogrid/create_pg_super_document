# RememberAllDependentForRebuilding

## Location
src/backend/commands/tablecmds.c: 13463 - 13688

## Overview
RememberAllDependentForRebuilding scans for all objects that depend on a specific column and records information necessary to recreate those objects after a column type change or expression modification.

## Definition


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
- : AlteredTableInfo structure to store rebuilding information
- : AlterTableType indicating the operation (AT_AlterColumnType or AT_SetExpression)
- : Relation containing the column being modified
- : Attribute number of the column being modified
- : Name of the column being modified (for error messages)

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