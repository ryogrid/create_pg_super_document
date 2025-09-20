# fmgr_sql_validator

## Location
[src/backend/catalog/pg_proc.c:811-977](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_proc.c#L811-L977)

## Overview
Validates SQL language functions by parsing and analyzing the function body to ensure syntactic correctness and proper return type matching.

## Definition

```c
Datum
fmgr_sql_validator(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the validator for SQL language functions in PostgreSQL. It performs comprehensive validation of SQL function definitions including syntax checking, semantic analysis, and return type verification.

The validator performs several validation steps:
1. **Type validation**: Ensures that return types and parameter types are valid for SQL functions (disallows most pseudo-types except RECORD, VOID, and polymorphic types)
2. **Syntax parsing**: Parses the function source code to catch syntax errors
3. **Semantic analysis**: For non-polymorphic functions, performs full semantic analysis including name resolution and type checking
4. **Return type verification**: Validates that the function body returns values compatible with the declared return type

The validator handles two different cases for function body storage:
- Traditional prosrc: Function source as SQL text
- Modern prosqlbody: Pre-parsed query tree stored in the catalog

For functions with polymorphic parameters, full semantic analysis is deferred to runtime since actual types cannot be resolved during definition time.

## Parameters / Member Variables
- Takes a single OID parameter via PG_FUNCTION_ARGS:
  - : OID of the SQL language function being validated

## Dependencies
- Functions called/Symbols referenced:
  - [CheckFunctionValidatorAccess](../C/CheckFunctionValidatorAccess.md): Verifies permission to validate this function
  - [get_typtype](../g/get_typtype.md): Gets the type category for pseudo-type checking
  - IsPolymorphicType: Checks if a type is polymorphic
  - [format_type_be](format_type_be.md): Formats type names for error messages
  - [sql_function_parse_error_callback](../s/sql_function_parse_error_callback.md): Error callback for enhanced error reporting
  - [pg_parse_query](../p/pg_parse_query.md): Parses SQL text into raw parse trees
  - [prepare_sql_fn_parse_info](../p/prepare_sql_fn_parse_info.md): Sets up parsing context for SQL functions
  - [pg_analyze_and_rewrite_withcb](../p/pg_analyze_and_rewrite_withcb.md): Performs semantic analysis and query rewriting
  - [sql_fn_parser_setup](../s/sql_fn_parser_setup.md): Parser setup hook for SQL function context
  - [AcquireRewriteLocks](../A/AcquireRewriteLocks.md): Acquires necessary locks for query rewriting
  - [pg_rewrite_query](../p/pg_rewrite_query.md): Applies rewrite rules to queries
  - [check_sql_fn_statements](../c/check_sql_fn_statements.md): Validates SQL function statement structure
  - [get_func_result_type](../g/get_func_result_type.md): Determines the actual return type of the function
  - [check_sql_fn_retval](../c/check_sql_fn_retval.md): Validates return value compatibility

- Called from (representative examples):
  - No direct references found in the codebase - typically registered as the validator for 'sql' language

## Notes and Other Information
- This validator respects the check_function_bodies GUC setting - body validation is skipped when disabled
- Polymorphic functions receive limited validation (syntax only) since type resolution requires runtime context
- The validator uses a custom error callback to provide better error messages with function context
- Both traditional prosrc and modern prosqlbody storage formats are supported
- Semantic validation includes full parse analysis, name resolution, and query rewriting
- Return type checking ensures compatibility between declared and actual return types
- Error reporting includes function name and source context for better debugging
- The validator can handle both simple expressions and complex multi-statement function bodies
- Lock acquisition during validation ensures consistency with concurrent DDL operations