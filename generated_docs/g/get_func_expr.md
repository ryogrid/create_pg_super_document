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
  - [get_rule_expr_paren](get_rule_expr_paren.md) (for deparsing arguments with parentheses handling)
  - [exprIsLengthCoercion](../e/exprIsLengthCoercion.md) (to detect length coercion functions)
  - [get_coercion_expr](get_coercion_expr.md) (to format cast expressions)
  - [get_func_sql_syntax](get_func_sql_syntax.md) (to handle special SQL syntax functions)
  - [generate_function_name](generate_function_name.md) (to resolve function names with overloading)
  - [get_rule_expr](get_rule_expr.md) (for deparsing individual arguments)
  - IsA, NamedArgExpr (for handling named arguments)
  - FUNC_MAX_ARGS (argument limit constant)
- Called from:
  - [get_rule_expr](get_rule_expr.md) (main expression deparsing dispatcher)

## Notes and Other Information
- Part of the rule deparsing system used for displaying views, rules, and constraints
- Handles function overloading by considering argument types and counts
- Supports PostgreSQL's variadic function syntax with VARIADIC keyword
- Enforces a maximum limit on function arguments (FUNC_MAX_ARGS)
- Special handling for type coercion functions that may appear as casts rather than function calls
- Named arguments are preserved in the deparsed output when present

## Simplified Source

```c
static void get_func_expr(FuncExpr *expr, deparse_context *context, bool showimplicit) {
    StringInfo buf = context->buf;
    Oid funcoid = expr->funcid;
    Oid argtypes[FUNC_MAX_ARGS];
    int nargs;
    List *argnames;
    bool use_variadic;

    // Handle implicit coercions - show only argument unless showimplicit is true
    if (expr->funcformat == COERCE_IMPLICIT_CAST && !showimplicit) {
        get_rule_expr_paren((Node *) linitial(expr->args), context, false, (Node *) expr);
        return;
    }

    // Handle explicit casts - show as cast expression
    if (expr->funcformat == COERCE_EXPLICIT_CAST || expr->funcformat == COERCE_IMPLICIT_CAST) {
        Node *arg = linitial(expr->args);
        Oid rettype = expr->funcresulttype;
        int32 coercedTypmod;

        // Check if this is a length-coercion function
        exprIsLengthCoercion((Node *) expr, &coercedTypmod);
        get_coercion_expr(arg, context, rettype, coercedTypmod, (Node *) expr);
        return;
    }

    // Try special SQL syntax functions first
    if (expr->funcformat == COERCE_SQL_SYNTAX) {
        if (get_func_sql_syntax(expr, context))
            return;
    }

    // Normal function: extract argument types and build function call
    if (list_length(expr->args) > FUNC_MAX_ARGS)
        ereport(ERROR, (errcode(ERRCODE_TOO_MANY_ARGUMENTS), errmsg("too many arguments")));

    // Extract argument types and names
    nargs = 0;
    argnames = NIL;
    foreach(l, expr->args) {
        Node *arg = (Node *) lfirst(l);
        if (IsA(arg, NamedArgExpr))
            argnames = lappend(argnames, ((NamedArgExpr *) arg)->name);
        argtypes[nargs] = exprType(arg);
        nargs++;
    }

    // Generate function name with proper overloading resolution
    appendStringInfo(buf, "%s(",
                     generate_function_name(funcoid, nargs, argnames, argtypes,
                                           expr->funcvariadic, &use_variadic,
                                           context->inGroupBy));

    // Format arguments with proper separators and VARIADIC keyword
    nargs = 0;
    foreach(l, expr->args) {
        if (nargs++ > 0)
            appendStringInfoString(buf, ", ");
        if (use_variadic && lnext(expr->args, l) == NULL)
            appendStringInfoString(buf, "VARIADIC ");
        get_rule_expr((Node *) lfirst(l), context, true);
    }
    appendStringInfoChar(buf, ')');
}
```