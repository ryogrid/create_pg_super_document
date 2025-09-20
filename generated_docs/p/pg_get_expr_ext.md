# pg_get_expr_ext

## Location
[src/backend/utils/adt/ruleutils.c:2646-2663](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L2646-L2663)

## Overview
Converts a stored pg_node_tree expression back into human-readable SQL text format with optional pretty-printing support.

## Definition

```c
Datum
pg_get_expr_ext(PG_FUNCTION_ARGS)
```
## Detailed Description
pg_get_expr_ext is a PostgreSQL system function that takes a pg_node_tree expression (stored as TEXT in system catalogs), a relation OID, and a pretty-printing flag, then returns the expression as human-readable SQL text. This function serves as the extended version of pg_get_expr, providing additional control over output formatting through the pretty-printing parameter. It acts as a thin wrapper around pg_get_expr_worker, which performs the actual expression deparsing work.

The function is commonly used by PostgreSQL's information schema views and system administration tools to convert internally stored expressions (like check constraints, default values, or index expressions) back into readable SQL format for display to users.

## Parameters / Member Variables
- : TEXT argument containing the pg_node_tree expression to be converted
- : OID of the relation that provides context for variable resolution (can be InvalidOid if no relation context needed)
- : BOOL flag indicating whether to apply pretty-printing formatting to the output

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP (macro for extracting TEXT argument)
  - PG_GETARG_OID (macro for extracting OID argument)
  - PG_GETARG_BOOL (macro for extracting BOOL argument)
  - GET_PRETTY_FLAGS (macro for converting boolean to pretty flags)
  - [pg_get_expr_worker](pg_get_expr_worker.md) (core expression deparsing function)
  - PG_RETURN_TEXT_P (macro for returning TEXT result)
- Called from:
  - SQL function pg_get_expr_ext() available to users

## Notes and Other Information
- This function is exposed as a SQL-callable system function in PostgreSQL
- Returns NULL if the expression cannot be successfully deparsed
- The pretty-printing flag controls formatting options like indentation and line breaks
- Located in src/backend/utils/adt/ruleutils.c:2646-2663