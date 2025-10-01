# alter_table_type_to_string

## Location
[src/backend/commands/tablecmds.c:6398-6542](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L6398-L6542)

## Overview
A utility function that converts AlterTableType enum values to their corresponding SQL command string representations for user-facing error messages and logging.

## Definition

```c
static const char *
alter_table_type_to_string(AlterTableType cmdtype)
```
## Detailed Description
This function serves as a mapping utility that translates internal ALTER TABLE operation type enumerations (AlterTableType) into human-readable SQL command strings. It provides standardized string representations for each type of ALTER TABLE operation, which are primarily used in error messages, permissions checking, and logging contexts. The function handles over 60 different ALTER TABLE operation types, returning the appropriate SQL syntax string for each operation or NULL for internal operations that don't correspond to actual SQL grammar.

## Parameters / Member Variables
- : The AlterTableType enumeration value representing the specific ALTER TABLE operation type to be converted to a string

## Dependencies
- Functions called/Symbols referenced:
  - [AlterTableType](../A/AlterTableType.md) (enum parameter)
  - All AT_* enumeration constants (AT_AddColumn, AT_DropColumn, AT_AddConstraint, etc.)
- Called from (representative examples):
  - [ATSimplePermissions](../A/ATSimplePermissions.md)

## Notes and Other Information
- Returns NULL for certain internal operation types that don't correspond to actual SQL grammar (marked with comments like "not real grammar")
- Covers all major ALTER TABLE operations including column operations, constraint operations, trigger operations, inheritance operations, and table property modifications
- The returned strings match standard PostgreSQL SQL syntax for ALTER TABLE commands
- This function is static and only used within the tablecmds.c module for consistent string representation across different ALTER TABLE processing functions

## Simplified Source

```c
static const char *alter_table_type_to_string(AlterTableType cmdtype) {
    switch (cmdtype) {
        // Column operations
        case AT_AddColumn:
        case AT_AddColumnToView:
            return "ADD COLUMN";
        case AT_DropColumn:
            return "DROP COLUMN";
        case AT_AlterColumnType:
            return "ALTER COLUMN ... SET DATA TYPE";
        case AT_ColumnDefault:
        case AT_CookedColumnDefault:
            return "ALTER COLUMN ... SET DEFAULT";
        case AT_DropNotNull:
            return "ALTER COLUMN ... DROP NOT NULL";
        case AT_SetNotNull:
            return "ALTER COLUMN ... SET NOT NULL";

        // Constraint operations
        case AT_AddConstraint:
        case AT_ReAddConstraint:
        case AT_ReAddDomainConstraint:
        case AT_AddIndexConstraint:
            return "ADD CONSTRAINT";
        case AT_DropConstraint:
            return "DROP CONSTRAINT";
        case AT_ValidateConstraint:
            return "VALIDATE CONSTRAINT";

        // Trigger operations
        case AT_EnableTrig:
            return "ENABLE TRIGGER";
        case AT_DisableTrig:
            return "DISABLE TRIGGER";
        case AT_EnableTrigAll:
            return "ENABLE TRIGGER ALL";
        case AT_DisableTrigAll:
            return "DISABLE TRIGGER ALL";

        // Table properties
        case AT_ChangeOwner:
            return "OWNER TO";
        case AT_SetTableSpace:
            return "SET TABLESPACE";
        case AT_SetLogged:
            return "SET LOGGED";
        case AT_SetUnLogged:
            return "SET UNLOGGED";

        // Inheritance operations
        case AT_AddInherit:
            return "INHERIT";
        case AT_DropInherit:
            return "NO INHERIT";

        // Partition operations
        case AT_AttachPartition:
            return "ATTACH PARTITION";
        case AT_DetachPartition:
            return "DETACH PARTITION";

        // Identity columns
        case AT_AddIdentity:
            return "ALTER COLUMN ... ADD IDENTITY";
        case AT_DropIdentity:
            return "ALTER COLUMN ... DROP IDENTITY";

        // Internal operations that don't correspond to SQL grammar
        case AT_AddIndex:
        case AT_ReAddIndex:
        case AT_CheckNotNull:
        case AT_ReAddComment:
        case AT_ReplaceRelOptions:
        case AT_ReAddStatistics:
            return NULL;

        // ... other cases omitted for brevity
        default:
            return NULL;
    }
}
```