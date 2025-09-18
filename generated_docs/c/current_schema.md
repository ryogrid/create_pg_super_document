# current_schema

## Location
src/backend/utils/adt/name.c: 279 - 293

## Overview
The current_schema function is a SQL function that returns the name of the first schema in the current search path.

## Definition


## Detailed Description
This function implements the SQL standard CURRENT_SCHEMA function. It retrieves the current search path and returns the name of the first schema in that path. The search path determines the order in which schemas are searched for unqualified object names.

If the search path is empty (NIL), the function returns NULL. If the first schema in the search path has been recently deleted, it also returns NULL. Otherwise, it converts the schema OID to its name and returns it as a SQL datum.

## Parameters / Member Variables
- This function takes no explicit parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL's standard function interface)

## Dependencies
- Functions called/Symbols referenced:
  - [fetch_search_path](../f/fetch_search_path.md): Retrieves the current search path as a list of schema OIDs
  - linitial_oid: Gets the first OID from the search path list
  - [get_namespace_name](../g/get_namespace_name.md): Converts a namespace OID to its name string
  - [list_free](../l/list_free.md): Frees the search path list memory
  - [CStringGetDatum](../C/CStringGetDatum.md): Converts C string to PostgreSQL Datum
  - DirectFunctionCall1: Directly calls a PostgreSQL function with one argument
  - namein: Input function for the name data type
  - PG_RETURN_DATUM: Macro to return a Datum from a PostgreSQL function
  - PG_RETURN_NULL: Macro to return NULL from a PostgreSQL function

- Called from (representative examples):
  - [ExecEvalSQLValueFunction](../E/ExecEvalSQLValueFunction.md): Used in expression evaluation

## Notes and Other Information
- This function is part of the SQL standard and provides schema context information
- Returns the first schema in the search path, which is where new objects would be created by default
- The function handles edge cases like empty search paths and deleted schemas gracefully
- The function is defined in src/backend/utils/adt/name.c alongside other name-related functions
- Returns the name as PostgreSQL's 'name' data type
- Essential for understanding the current schema context for unqualified object references