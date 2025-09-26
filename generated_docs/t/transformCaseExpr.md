# transformCaseExpr

## Location
src/backend/parser/parse_expr.c: 1632 - 1771

## Overview
Transforms a CASE expression node (CaseExpr) during parsing by handling both simple and searched CASE forms, processing WHEN clauses, and performing type resolution and coercion.

## Definition
```c
static Node *transformCaseExpr(ParseState *pstate, CaseExpr *c)
```

## Detailed Description
The transformCaseExpr function handles transformation of CASE expressions during SQL parsing, supporting both simple CASE (CASE expr WHEN value THEN result) and searched CASE (CASE WHEN condition THEN result) forms. The function performs comprehensive type checking, creates placeholder expressions for simple CASE forms, and ensures all result expressions have compatible types.

Key processing steps:
1. **Test expression handling**: Transforms the optional test expression, handling untyped literals and collation assignment
2. **Placeholder creation**: For simple CASE forms, creates a CaseTestExpr placeholder to represent the test value
3. **WHEN clause transformation**: Processes each WHEN clause, expanding simple CASE forms to equality comparisons
4. **Type resolution**: Determines a common result type for all WHEN and ELSE expressions
5. **Type coercion**: Coerces all result expressions to the common type
6. **SRF validation**: Ensures no set-returning functions are used within the CASE expression

The function handles both shorthand (simple) and full (searched) CASE syntax by internally converting simple CASE to searched CASE using equality comparisons.

## Parameters / Member Variables
- `pstate`: ParseState context containing parsing state information and error handling context
- `c`: CaseExpr node containing the test expression, WHEN clauses, default result, and location information

## Dependencies
- Functions called/Symbols referenced:
  - CaseExpr, CaseWhen, CaseTestExpr (struct types for CASE expressions)
  - A_Const (struct type for constant values)
  - transformExprRecurse (recursively transforms expression nodes)
  - coerce_to_common_type, coerce_to_boolean (type coercion functions)
  - assign_expr_collations (assigns collations to expressions)
  - makeSimpleA_Expr (creates simple A_Expr nodes for equality)
  - select_common_type (determines common type from expression list)
  - exprType, exprTypmod, exprCollation, exprLocation (expression metadata functions)
  - AEXPR_OP (A_Expr operation type constant)
- Called from:
  - transformExprRecurse (main expression transformation dispatcher)

## Notes and Other Information
- This function is part of the SQL parser's expression transformation pipeline
- Supports both simple CASE (with test expression) and searched CASE (without test expression) forms
- For simple CASE, creates equality comparisons using makeSimpleA_Expr
- Handles untyped literals in test expressions by coercing to TEXT type
- Uses CaseTestExpr placeholders to avoid re-evaluating the test expression
- Performs type resolution to find a common type for all result expressions
- Validates that set-returning functions are not used within CASE expressions
- The default result is given priority in type resolution (though this is noted as potentially questionable behavior)
- The function is static, indicating it's only used within the parse_expr.c module