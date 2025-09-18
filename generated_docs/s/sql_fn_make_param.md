# sql_fn_make_param

## Location
[src/backend/executor/functions.c:410-439](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/functions.c#L410-L439)

## Overview
Constructs a Param node for a given parameter number in SQL function parsing context.

## Definition


## Detailed Description
This function creates a Param node representing a function parameter () with the appropriate type information and collation settings. It constructs an external parameter node with the correct parameter type derived from the function's argument types, handles collation inheritance from the function's input collation when applicable, and sets up all necessary fields for proper parameter handling during query execution.

## Parameters / Member Variables
- : SQLFunctionParseInfo containing function metadata and argument type information
- : Parameter number (1-based index) for which to create the Param node
- : Source location of the parameter reference for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - SQLFunctionParseInfoPtr
  - Param
  - PARAM_EXTERN
  - [get_typcollation](../g/get_typcollation.md)
- Called from (representative examples):
  - [sql_fn_param_ref](sql_fn_param_ref.md) (src/backend/executor/functions.c:403)
  - [sql_fn_resolve_param_name](sql_fn_resolve_param_name.md) (src/backend/executor/functions.c:451)

## Notes and Other Information
- Creates PARAM_EXTERN type parameters which represent externally supplied parameter values
- Uses 1-based parameter numbering but converts to 0-based array indexing for type lookup
- Sets paramtypmod to -1 indicating no specific type modifier
- Handles collation inheritance where function input collation can override type-derived collation
- The location field enables accurate error reporting when parameter issues occur during execution
- All created parameters are properly typed using the function's declared argument types from pinfo->argtypes