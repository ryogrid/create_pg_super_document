# show_config_by_name

## Location
[src/backend/utils/misc/guc_funcs.c:807-824](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc_funcs.c#L807-L824)

## Overview
A PostgreSQL system function that retrieves the current value of a configuration parameter by name, equivalent to the SHOW SQL command.

## Definition

```c
struct config_generic **guc_vars;
```
## Detailed Description
This function provides programmatic access to PostgreSQL configuration parameter values from within SQL queries. It takes a configuration parameter name as input and returns the current value of that parameter as text. The function serves as a functional equivalent to the SHOW SQL command, allowing configuration values to be retrieved in SELECT statements and other SQL contexts where expressions are needed.

The function follows PostgreSQL's standard function calling convention using the PG_FUNCTION_ARGS macro and returns a Datum containing the parameter value as a text type. It internally uses GetConfigOptionByName() to retrieve the actual parameter value and converts it to the appropriate PostgreSQL text format for return.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure
  - Arg 0:  (text) - The name of the configuration parameter to retrieve

## Dependencies
- Functions called/Symbols referenced:
  - TextDatumGetCString
  - [GetConfigOptionByName](../G/GetConfigOptionByName.md)
  - cstring_to_text
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - SQL queries using the function (no direct C references found)

## Notes and Other Information
- This function is exposed to SQL as a system function, typically used in queries like SELECT show_config_by_name('shared_buffers')
- The function does not handle missing configuration parameters gracefully - it will raise an error if the parameter name is invalid
- For a version that handles missing parameters without errors, use show_config_by_name_missing_ok()
- The function is registered in the PostgreSQL system catalogs and can be called from any SQL context
- Return value is always of PostgreSQL text type, regardless of the underlying parameter's data type