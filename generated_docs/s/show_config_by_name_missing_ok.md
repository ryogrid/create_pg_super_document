# show_config_by_name_missing_ok

## Location
[src/backend/utils/misc/guc_funcs.c:825-845](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc_funcs.c#L825-L845)

## Overview
A PostgreSQL system function that retrieves the current value of a configuration parameter by name with optional error suppression for non-existent parameters.

## Definition

```c
struct config_generic **guc_vars;
```
## Detailed Description
This function provides a more flexible version of show_config_by_name() that can gracefully handle requests for non-existent configuration parameters. It takes both a configuration parameter name and a boolean flag indicating whether to suppress errors for missing parameters. When missing_ok is true and the parameter doesn't exist, the function returns NULL instead of raising an error, making it suitable for conditional configuration queries where parameter existence is uncertain.

The function serves as a robust alternative to the SHOW SQL command, particularly useful in system administration scripts and diagnostic queries where configuration parameters might vary between PostgreSQL versions or installations. It maintains the same return format as show_config_by_name() but provides additional error handling capabilities.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure
  - Arg 0:  (text) - The name of the configuration parameter to retrieve
  - Arg 1:  (boolean) - If true, return NULL for non-existent parameters instead of raising an error

## Dependencies
- Functions called/Symbols referenced:
  - TextDatumGetCString
  - PG_GETARG_BOOL
  - [GetConfigOptionByName](../G/GetConfigOptionByName.md)
  - [cstring_to_text](../c/cstring_to_text.md)
  - PG_RETURN_TEXT_P
  - PG_RETURN_NULL
- Called from (representative examples):
  - SQL queries using the function (no direct C references found)

## Notes and Other Information
- This function is exposed to SQL as a system function, typically used in queries like SELECT show_config_by_name_missing_ok('custom_parameter', true)
- When missing_ok is false, the function behaves identically to show_config_by_name()
- The function is particularly useful for checking the existence and value of custom or extension-specific parameters
- Return value is either PostgreSQL text type (for existing parameters) or NULL (for missing parameters when missing_ok=true)
- This function provides better compatibility across different PostgreSQL versions where parameter names might have changed

## Simplified Source

```c
Datum
show_config_by_name_missing_ok(PG_FUNCTION_ARGS)
{
    // Get function arguments
    char *param_name = TextDatumGetCString(PG_GETARG_DATUM(0));
    bool missing_ok = PG_GETARG_BOOL(1);

    // Retrieve the configuration value (with error suppression if requested)
    char *param_value = GetConfigOptionByName(param_name, NULL, missing_ok);

    // Return NULL if parameter doesn't exist and missing_ok is true
    if (param_value == NULL)
        PG_RETURN_NULL();

    // Convert to PostgreSQL text type and return
    PG_RETURN_TEXT_P(cstring_to_text(param_value));
}
```