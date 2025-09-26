# get_func_expr

## Location
[src/backend/utils/adt/ruleutils.c:10465-10560](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L10465-L10560)

## Overview
Parses back a FuncExpr (function expression) node into its human-readable string representation, handling various function call formats including implicit/explicit casts and special SQL syntaxes.

## Definition

```c
static void
get_func_expr(FuncExpr *expr, deparse_context *context,
			  bool showimplicit)
```
## Detailed Description
This static function is a core component of PostgreSQL's rule deparsing system that converts FuncExpr nodes back to SQL text. The function handles multiple function call formats:

1. **Implicit coercions**: When a function represents an implicit type cast, it can either show just the argument (default) or the full cast (when showimplicit is true)
2. **Explicit casts**: Displays cast expressions using CAST() syntax or :: operator
3. **SQL syntax functions**: Special functions that use non-standard SQL syntax (e.g., EXTRACT, OVERLAY)
4. **Normal functions**: Standard function calls displayed as funcname(args)

The function extracts argument types, handles named arguments, manages variadic functions, and ensures proper comma separation between arguments. It also enforces the FUNC_MAX_ARGS limit on function arguments.

## Parameters / Member Variables
- : Pointer to the FuncExpr node containing the function call to be deparsed
- : Deparse context containing the output buffer and formatting preferences
- : Boolean flag indicating whether to show implicit coercions explicitly

## Dependencies
- Functions called/Symbols referenced:
  - COERCE_IMPLICIT_CAST, COERCE_EXPLICIT_CAST, COERCE_SQL_SYNTAX (function format constants)
  - get_rule_expr_paren (for deparsing arguments with parentheses handling)
  - exprIsLengthCoercion (to detect length coercion functions)
  - get_coercion_expr (to format cast expressions)
  - get_func_sql_syntax (to handle special SQL syntax functions)
  - generate_function_name (to resolve function names with overloading)
  - get_rule_expr (for deparsing individual arguments)
  - IsA, NamedArgExpr (for handling named arguments)
  - FUNC_MAX_ARGS (argument limit constant)
- Called from:
  - get_rule_expr (main expression deparsing dispatcher)

## Notes and Other Information
- Part of the rule deparsing system used for displaying views, rules, and constraints
- Handles function overloading by considering argument types and counts
- Supports PostgreSQL's variadic function syntax with VARIADIC keyword
- Enforces a maximum limit on function arguments (FUNC_MAX_ARGS)
- Special handling for type coercion functions that may appear as casts rather than function calls
- Named arguments are preserved in the deparsed output when present