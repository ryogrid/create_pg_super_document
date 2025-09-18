# ATExecColumnDefault

## Location
[src/backend/commands/tablecmds.c:7908-7993](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L7908-L7993)

## Overview
ATExecColumnDefault handles both SET DEFAULT and DROP DEFAULT operations for table columns during ALTER TABLE commands, managing default value expressions and validating column constraints.

## Definition


## Detailed Description
This function implements the execution phase for ALTER TABLE ALTER COLUMN SET/DROP DEFAULT commands. It performs comprehensive validation and handles both setting new defaults and removing existing ones:

1. **Column Validation**: Verifies the target column exists and is not a system column (attnum > 0).

2. **Special Column Restrictions**: 
   - Prevents modification of identity columns, suggesting appropriate ALTER IDENTITY commands instead
   - Prevents modification of generated columns, suggesting ALTER EXPRESSION commands instead
   - Provides helpful error hints for the correct syntax to use

3. **Default Removal**: Always removes any existing default value using RemoveAttrDefault() with RESTRICT mode for safety.

4. **Default Addition**: If a new default is provided (SET DEFAULT), creates a RawColumnDefault structure and uses AddRelationNewConstraints() to add the new default expression.

5. **Operation Tracking**: Distinguishes between user-initiated DROP DEFAULT and internal default removal that's preparatory to setting a new default.

The function ensures data integrity by validating column types and providing clear error messages for unsupported operations.

## Parameters / Member Variables
- : The relation being altered
- : Name of the column whose default is being modified
- : New default expression (NULL for DROP DEFAULT operations)
- : Lock mode for the operation (currently unused in function body)

## Dependencies
- Functions called/Symbols referenced:
  - [get_attnum](../g/get_attnum.md) (to resolve column name to attribute number)
  - [RemoveAttrDefault](../R/RemoveAttrDefault.md) (to remove existing default values)
  - [AddRelationNewConstraints](AddRelationNewConstraints.md) (to add new default constraints)
  - ObjectAddressSubSet (to create return address for the modified column)
  - TupleDescAttr (macro to access column attributes)
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md) (main ALTER TABLE command execution dispatcher)

## Notes and Other Information
- Supports both SET DEFAULT and DROP DEFAULT operations based on whether newDefault is NULL
- Provides comprehensive validation for identity and generated columns with helpful error hints
- Uses DROP_RESTRICT mode when removing defaults to ensure safety (though no dependencies are expected)
- The function reuses AddRelationNewConstraints() which was designed for CREATE TABLE but works with single-item lists
- Returns the ObjectAddress of the modified column for dependency tracking and event system integration
- Identity columns require specific ALTER IDENTITY syntax, while generated columns require ALTER EXPRESSION syntax