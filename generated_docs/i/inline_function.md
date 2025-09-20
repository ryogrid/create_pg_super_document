# inline_function

## Location
[src/backend/optimizer/util/clauses.c:4551-4906](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L4551-L4906)

## Overview
Attempts to expand a SQL function call inline by substituting the function body directly into the calling query, avoiding function call overhead and exposing optimization opportunities.

## Definition

```c
structs and must use all of the function
	 * parameters (this is overkill, but an exact analysis is hard).
	 */
	if (funcform->provolatile == PROVOLATILE_IMMUTABLE &&
		contain_mutable_functions(newexpr))
		goto fail;
```
## Detailed Description
This function performs function inlining optimization for SQL-language functions. It attempts to replace function calls with their actual implementation when the function body is a simple "SELECT expression". This optimization eliminates the per-call overhead of SQL functions and can expose additional constant-folding opportunities.

The function includes comprehensive safety checks to prevent problematic inlining scenarios: recursive functions (tracked via context->active_fns), functions with multiple parameter usage of volatile/expensive expressions, functions that would change volatility/strictness properties, and functions with context-dependent nodes. It parses the function body, validates it's a simple SELECT statement, performs parameter substitution, and recursively optimizes the result.

The inlining process involves several phases: validation of function properties, parsing the function source code (handling both prosrc and prosqlbody), parameter substitution with usage counting, cost analysis for multiply-used parameters, and final collation handling.

## Parameters / Member Variables
- : OID of the function to inline
- : Expected result type OID of the function
- : Collation ID for the result
- : Collation ID for the inputs
- : List of actual function arguments
- : Whether the function is variadic
- : HeapTuple containing the function's catalog entry
- : Evaluation context containing active function tracking and optimization settings

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_proc (function catalog entry structure)
  - [heap_attisnull](../h/heap_attisnull.md) (checks for NULL attributes)
  - [prepare_sql_fn_parse_info](../p/prepare_sql_fn_parse_info.md) (prepares SQL function parsing context)
  - [pg_parse_query](../p/pg_parse_query.md) (parses SQL text into parse trees)
  - [sql_fn_parser_setup](../s/sql_fn_parser_setup.md) (configures parser for SQL functions)
  - [transformTopLevelStmt](../t/transformTopLevelStmt.md) (transforms parse tree to Query)
  - [check_sql_fn_retval](../c/check_sql_fn_retval.md) (validates function return value)
  - [substitute_actual_parameters](../s/substitute_actual_parameters.md) (replaces parameter references)
  - [contain_volatile_functions](../c/contain_volatile_functions.md), contain_mutable_functions, contain_nonstrict_functions (volatility checks)
  - [eval_const_expressions_mutator](../e/eval_const_expressions_mutator.md) (recursive optimization)
- Called from:
  - [simplify_function](../s/simplify_function.md) (main function simplification routine)

## Notes and Other Information
- Returns NULL if inlining is not possible, otherwise returns the inlined expression
- Only inlines SQL-language functions that are simple SELECT expressions
- Prevents recursive inlining by tracking active functions in context
- Uses temporary memory context to avoid leaks during parsing
- Enforces parameter usage rules: strict functions must use all parameters, expensive/volatile parameters cannot be used multiple times
- Handles both prosrc (text) and prosqlbody (parsed) function representations
- Records plan dependency on inlined functions for proper invalidation
- Maintains proper collation information in the result
- Located in src/backend/optimizer/util/clauses.c at lines 4551-4906