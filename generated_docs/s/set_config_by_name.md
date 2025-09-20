# set_config_by_name

## Location
[src/backend/utils/misc/guc_funcs.c:332-381](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc_funcs.c#L332-L381)

## Overview
SQL-callable function that provides access to PostgreSQL's SET command functionality, allowing configuration variables to be set from within SQL queries and returning the new value.

## Definition

```c
Datum
set_config_by_name(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a SQL-accessible wrapper around PostgreSQL's configuration setting functionality. It implements the set_config() SQL function that can be called from SQL queries to modify GUC (Grand Unified Configuration) parameters.

The function takes up to three arguments:
1. **Parameter name** (required): The name of the GUC variable to set
2. **Value** (optional): The new value to set, or NULL to reset to default
3. **is_local flag** (optional): Whether to make this a transaction-local setting

Key behaviors:
- Returns the new current value of the parameter after setting it
- Handles NULL values appropriately (NULL value means RESET, NULL is_local defaults to false)
- Applies proper permission checking based on superuser status
- Supports both session-level and transaction-local modifications

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro for PostgreSQL function argument handling:
  - Argument 0: Parameter name (text, required)
  - Argument 1: New value (text, optional - NULL means reset)  
  - Argument 2: is_local flag (boolean, optional - defaults to false)

## Dependencies
- Functions called/Symbols referenced:
  - set_config_option
  - [GetConfigOptionByName](../G/GetConfigOptionByName.md)
  - TextDatumGetCString
  - cstring_to_text
  - superuser
- Called from (representative examples):
  - Available as SQL function set_config() but no direct C references found

## Notes and Other Information
- Implements the SQL function set_config(setting_name, new_value, is_local)
- Returns text representation of the new parameter value after setting
- Provides comprehensive NULL argument handling with appropriate error messages
- Uses standard PostgreSQL function argument macros (PG_GETARG_*, PG_RETURN_*)
- Enables dynamic configuration changes from within SQL queries and stored procedures