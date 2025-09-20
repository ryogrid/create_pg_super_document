# prepare_sql_fn_parse_info

## Location
[src/backend/executor/functions.c:176-264](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/functions.c#L176-L264)

## Overview
Prepares a SQLFunctionParseInfo structure for parsing a SQL function body, including resolution of actual types for polymorphic arguments.

## Definition

```c
SQLFunctionParseInfoPtr
prepare_sql_fn_parse_info(HeapTuple procedureTuple,
						  Node *call_expr,
						  Oid inputCollation)
```
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
  - [get_call_expr_argtype](../g/get_call_expr_argtype.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [get_func_input_arg_names](../g/get_func_input_arg_names.md)
- Called from (representative examples):
  - [fmgr_sql_validator](../f/fmgr_sql_validator.md) (src/backend/catalog/pg_proc.c:933)
  - [init_sql_fcache](../i/init_sql_fcache.md) (src/backend/executor/functions.c:657)
  - [inline_function](../i/inline_function.md) (src/backend/optimizer/util/clauses.c:4671)
  - [inline_set_returning_function](../i/inline_set_returning_function.md) (src/backend/optimizer/util/clauses.c:5229)

## Notes and Other Information
- The function handles polymorphic type resolution by examining the call expression to determine actual argument types
- If call_expr is NULL and polymorphic arguments exist, the function will raise an error
- The function allocates memory for the parse info structure and copies argument type information from the procedure definition
- Argument names are extracted from the pg_proc entry if available, with proper validation of array bounds