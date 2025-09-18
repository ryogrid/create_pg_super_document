# ExecInitCheck

## Location
src/backend/executor/execExpr.c: 307 - 326

## Overview
ExecInitCheck prepares a check constraint for execution by ExecCheck, treating NULL results as TRUE in accordance with SQL's CHECK constraint semantics.

## Definition


## Detailed Description
ExecInitCheck is a specialized variant of expression compilation designed specifically for CHECK constraints. The key difference from ExecInitQual is the handling of NULL results: while ExecInitQual treats NULL as FALSE (appropriate for WHERE clauses), ExecInitCheck treats NULL as TRUE, which matches SQL's specification that NULL constraint conditions are not constraint violations.

The function implements this by converting the implicit-AND list of expressions into an explicit AND expression tree using make_ands_explicit(), then compiling it normally with ExecInitExpr. This approach leverages the regular boolean AND evaluation logic, which naturally treats NULL results according to SQL's three-valued logic rules (NULL AND TRUE = NULL, which represents a passing constraint).

Unlike ExecInitQual's optimized short-circuit evaluation that immediately fails on NULL, ExecInitCheck must evaluate all expressions to properly handle the case where some expressions are NULL but others might be FALSE (which would represent a constraint violation).

## Parameters / Member Variables
- : A List of expression nodes representing the check constraint in implicit-AND format. Returns NULL if the list is empty (NIL), representing a constraint that always passes.
- : The PlanState node that owns this check constraint expression.

## Dependencies
- Functions called/Symbols referenced:
  - [make_ands_explicit](../m/make_ands_explicit.md) (converts implicit-AND list to explicit AND expression)
  - [ExecInitExpr](ExecInitExpr.md) (compiles the resulting explicit AND expression)
- Called from (representative examples):
  - [ExecPrepareCheck](ExecPrepareCheck.md) (for preparing standalone check constraints)
  - ExecProcNode (header inclusion)

## Notes and Other Information
- Designed specifically for CHECK constraint evaluation following SQL semantics
- NULL constraint results are treated as TRUE (constraint passes), unlike WHERE clause evaluation
- Accepts input in implicit-AND format like ExecInitQual, but users with explicit-AND expressions can apply ExecInitExpr directly
- Uses standard boolean AND evaluation rather than the optimized qualification evaluation used by ExecInitQual
- Must evaluate all subexpressions rather than short-circuiting on NULL to properly distinguish between NULL (pass) and FALSE (fail) results
- Less commonly used than ExecInitQual since CHECK constraints are evaluated less frequently than WHERE conditions
- The resulting ExprState should be used with ExecCheck, which understands the CHECK constraint semantics