# generate_function_name

## Location
[src/backend/utils/adt/ruleutils.c:12927-13031](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L12927-L13031)

## Overview
Computes the properly qualified and quoted name to display for a function specified by OID, considering argument types, variadic behavior, and function resolution rules to determine if schema qualification is needed.

## Definition

```c
static char *
generate_function_name(Oid funcid, int nargs, List *argnames, Oid *argtypes,
					   bool has_variadic, bool *use_variadic_p,
					   bool inGroupBy)
```
## Detailed Description
This function generates an appropriate display name for a function call, implementing sophisticated logic to determine whether schema qualification is necessary. It considers function overloading resolution rules by checking if the unqualified function name with the given arguments would resolve to the same function. The function also handles variadic functions properly, determining whether the VARIADIC keyword should be displayed, and includes special handling for functions like "cube" and "rollup" that require qualification in GROUP BY contexts due to parser limitations.

## Parameters / Member Variables
- `funcid`: The OID of the function to generate a name for
- `nargs`: The number of arguments being passed to the function
- `argnames`: List of argument names (can be NIL if no named arguments)
- `argtypes`: Array of argument type OIDs
- `has_variadic`: True if variadic arguments have been merged into an array
- `use_variadic_p`: Output parameter set to indicate whether VARIADIC should be printed; can be NULL for non-FuncExpr cases
- `inGroupBy`: True if generating the name for use in a GROUP BY clause

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1
  - HeapTupleIsValid
  - GETSTRUCT
  - NameStr
  - func_get_detail
  - makeString
  - list_make1
  - get_namespace_name_or_temp
  - quote_qualified_identifier
  - ReleaseSysCache
  - elog
- Called from (representative examples):
  - pg_get_triggerdef_worker
  - pg_get_functiondef
  - get_func_expr
  - get_agg_expr_helper
  - get_windowfunc_expr_helper
  - get_tablesample_def

## Notes and Other Information
- This is a static function local to ruleutils.c
- Implements intelligent qualification logic based on function resolution rules
- Handles the complexity of PostgreSQL's function overloading system
- Special cases exist for "cube" and "rollup" functions in GROUP BY contexts
- Critical for generating correct SQL when functions might be overloaded
- The returned string is palloc'd and must be freed by the caller
- Part of PostgreSQL's expression deparsing infrastructure