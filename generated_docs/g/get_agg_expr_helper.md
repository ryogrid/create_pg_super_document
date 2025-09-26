# get_agg_expr_helper

## Location
[src/backend/utils/adt/ruleutils.c:10573-10698](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L10573-L10698)

## Overview
The core implementation function for deparsing aggregate expressions (Aggref nodes), supporting various aggregate types including standard aggregates, ordered-set aggregates, combining aggregates, and JSON aggregates.

## Definition

```c
static void
get_agg_expr_helper(Aggref *aggref, deparse_context *context,
					Aggref *original_aggref, const char *funcname,
					const char *options, bool is_json_objectagg)
```
## Detailed Description
This function serves as the comprehensive implementation for converting aggregate expressions back to SQL text. It handles multiple complex scenarios:

1. **Combining aggregates**: For parallel query execution, it resolves partial aggregates and combines their transition states
2. **Partial aggregates**: Marks aggregates with PARTIAL keyword when appropriate for parallel processing
3. **Ordered-set aggregates**: Handles special syntax with WITHIN GROUP (ORDER BY ...) clause
4. **Standard aggregates**: Processes regular aggregate functions with optional DISTINCT, ORDER BY, and FILTER clauses
5. **JSON aggregates**: Special handling for JSON object aggregation with key-value syntax
6. **Variadic functions**: Manages aggregates that accept variable numbers of arguments

The function extracts argument types, resolves function names, formats arguments with proper separators, and adds appropriate SQL keywords and clauses based on the aggregate's properties.

## Parameters / Member Variables
- : Pointer to the Aggref node containing the aggregate expression to be deparsed
- : Deparse context containing the output buffer and formatting preferences  
- : Pointer to the original Aggref node, used for context in combining aggregates
- : Optional function name override (NULL to auto-resolve)
- : Optional additional SQL options string to append
- : Boolean flag indicating special JSON object aggregate formatting

## Dependencies
- Functions called/Symbols referenced:
  - DO_AGGSPLIT_COMBINE, DO_AGGSPLIT_SKIPFINAL (aggregate splitting macros)
  - [get_agg_combine_expr](get_agg_combine_expr.md) (for handling combining aggregates)
  - [get_aggregate_argtypes](get_aggregate_argtypes.md) (to extract argument types)
  - [generate_function_name](generate_function_name.md) (to resolve function names with overloading)
  - AGGKIND_IS_ORDERED_SET (macro to detect ordered-set aggregates)
  - [get_rule_expr](get_rule_expr.md) (for deparsing arguments and filter expressions)
  - [get_rule_orderby](get_rule_orderby.md) (for deparsing ORDER BY clauses)
  - [resolve_special_varno](../r/resolve_special_varno.md) (for resolving special variables in combining aggregates)
  - FUNC_MAX_ARGS (maximum function arguments constant)
- Called from:
  - [get_agg_expr](get_agg_expr.md) (standard aggregate expression wrapper)
  - [get_json_agg_constructor](get_json_agg_constructor.md) (JSON aggregate constructor)

## Notes and Other Information
- Part of PostgreSQL's rule deparsing system used for displaying views, rules, and constraints
- Handles PostgreSQL's parallel query execution by managing partial and combining aggregates
- Supports advanced aggregate features like DISTINCT, ORDER BY, FILTER, and WITHIN GROUP clauses  
- Special formatting for JSON object aggregates using key:value syntax instead of comma separation
- Manages variadic aggregate functions with proper VARIADIC keyword placement
- The aggstar field handles COUNT(*) syntax for zero-argument aggregates
- Filter conditions are formatted with FILTER (WHERE ...) syntax when present