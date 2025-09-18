# prepare_sql_fn_parse_info

## Location
src/backend/executor/functions.c: 176 - 264

## Overview
Prepares a SQLFunctionParseInfo structure for parsing a SQL function body, including resolution of actual types for polymorphic arguments.

## Definition


## Detailed Description
This function creates and initializes a SQLFunctionParseInfo structure that contains all the necessary information for parsing and executing SQL function bodies. The function extracts function metadata from the pg_proc system catalog entry, resolves polymorphic argument types using the provided call expression, and collects argument names and types. This preparation is essential for the SQL function parser to correctly handle parameter references and type checking within the function body.

## Parameters / Member Variables
- : HeapTuple containing the pg_proc catalog entry for the function
- : Node representing the function call expression (can be NULL, but will fail if polymorphic arguments exist)
- : Oid specifying the input collation to use for the function

## Dependencies
- Functions called/Symbols referenced:
  - SQLFunctionParseInfoPtr
  - Form_pg_proc
  - SQLFunctionParseInfo
  - IsPolymorphicType
  - get_call_expr_argtype
  - SysCacheGetAttr
  - get_func_input_arg_names
- Called from (representative examples):
  - fmgr_sql_validator (src/backend/catalog/pg_proc.c:933)
  - init_sql_fcache (src/backend/executor/functions.c:657)
  - inline_function (src/backend/optimizer/util/clauses.c:4671)
  - inline_set_returning_function (src/backend/optimizer/util/clauses.c:5229)

## Notes and Other Information
- The function handles polymorphic type resolution by examining the call expression to determine actual argument types
- If call_expr is NULL and polymorphic arguments exist, the function will raise an error
- The function allocates memory for the parse info structure and copies argument type information from the procedure definition
- Argument names are extracted from the pg_proc entry if available, with proper validation of array bounds