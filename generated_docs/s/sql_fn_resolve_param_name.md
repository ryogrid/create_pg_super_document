# sql_fn_resolve_param_name

## Location
src/backend/executor/functions.c: 440 - 463

## Overview
Searches for a function parameter by name and constructs a Param node if found, serving as a helper function for SQL function parameter resolution during parsing.

## Definition


## Detailed Description
This function implements parameter name resolution for SQL functions by searching through the argument names array in the parse info structure. When a parameter with the specified name is found, it delegates to sql_fn_make_param to construct the appropriate Param node. This is a key component of the SQL function parsing infrastructure that enables named parameter references in function bodies.

## Parameters / Member Variables
- : Pointer to SQLFunctionParseInfo structure containing function parsing context including argument names and count
- : Name of the parameter to search for
- : Source location information for error reporting and node construction

## Dependencies
- Functions called/Symbols referenced:
  - [sql_fn_make_param](sql_fn_make_param.md)
  - SQLFunctionParseInfoPtr (type)
- Called from (representative examples):
  - [sql_fn_post_column_ref](sql_fn_post_column_ref.md)

## Notes and Other Information
- Returns NULL if no parameter with the given name is found or if argnames is NULL
- Uses 1-based parameter numbering when calling sql_fn_make_param (i + 1)
- Performs simple string comparison to match parameter names
- Part of the SQL function parsing hook infrastructure for resolving parameter references