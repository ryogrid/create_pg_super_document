# ExtractSetVariableArgs

## Location
[src/backend/utils/misc/guc_funcs.c:167-191](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc_funcs.c#L167-L191)

## Overview
Extracts and returns the string value to be assigned for a VariableSetStmt, handling different types of SET operations and returning NULL for RESET operations.

## Definition

```c
struct config_generic *record;
```
## Detailed Description
This function is a utility that extracts the appropriate value from a VariableSetStmt based on its kind. It serves as a central point for converting SET statement arguments into string values that can be used by the GUC (Grand Unified Configuration) system. The function handles two main cases:

- **VAR_SET_VALUE**: Uses flatten_set_variable_args to convert the argument list into a single string value
- **VAR_SET_CURRENT**: Retrieves the current value of the specified configuration parameter
- **Other cases**: Returns NULL (typically for RESET operations)

The returned string is palloc'd and must be freed by the caller.

## Parameters / Member Variables
- : Pointer to VariableSetStmt structure containing the SET command details

## Dependencies
- Functions called/Symbols referenced:
  - [flatten_set_variable_args](../f/flatten_set_variable_args.md)
  - [GetConfigOptionByName](../G/GetConfigOptionByName.md)
- Called from (representative examples):
  - [ExecSetVariableStmt](ExecSetVariableStmt.md) (in src/backend/utils/misc/guc_funcs.c:63)
  - [AlterSetting](../A/AlterSetting.md) (in src/backend/catalog/pg_db_role_setting.c:32)
  - [update_proconfig_value](../u/update_proconfig_value.md) (in src/backend/commands/functioncmds.c:657)
  - [AlterSystemSetConfigFile](../A/AlterSystemSetConfigFile.md) (in src/backend/utils/misc/guc.c:4634)

## Notes and Other Information
- This function is exported and used by various ALTER commands like ALTER ROLE SET
- Returns palloc'd memory that must be freed by caller
- Designed to handle the complexity of converting different argument formats into consistent string values
- Serves as an abstraction layer between SET statement parsing and GUC value assignment

## Simplified Source

```c
char *ExtractSetVariableArgs(VariableSetStmt *stmt)
{
    switch (stmt->kind)
    {
        case VAR_SET_VALUE:
            // Convert argument list to string value
            return flatten_set_variable_args(stmt->name, stmt->args);

        case VAR_SET_CURRENT:
            // Get current value of the parameter
            return GetConfigOptionByName(stmt->name, NULL, false);

        default:
            // RESET operations and others return NULL
            return NULL;
    }
}
```