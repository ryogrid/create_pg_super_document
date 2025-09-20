# isAssignmentIndirectionExpr

## Location
[src/backend/executor/execExpr.c:3309-3345](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExpr.c#L3309-L3345)

## Overview
Determines if an expression is a nested assignment indirection that requires the old element value to be passed down via the CaseTestExpr mechanism.

## Definition

```c
static bool
isAssignmentIndirectionExpr(Expr *expr)
```
## Detailed Description
isAssignmentIndirectionExpr analyzes expressions to identify nested assignment situations where the replacement expression needs access to the current/old value being replaced. This occurs in complex assignment patterns where FieldStore or SubscriptingRef expressions contain CaseTestExpr nodes, indicating they need the previous value to compute the new value.

The function handles several expression types including FieldStore (for record field assignments), SubscriptingRef (for array/container assignments), and can look through type coercion expressions (CoerceToDomain and RelabelType) that might wrap the actual assignment expressions. This is particularly important for array-of-domain types where domain coercion wraps the assignment expression.

## Parameters / Member Variables
- : The expression to analyze for assignment indirection patterns

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for node type checking)
  - [isAssignmentIndirectionExpr](isAssignmentIndirectionExpr.md) (recursive calls for coercion expressions)
- Called from (representative examples):
  - [ExecInitSubscriptingRef](../E/ExecInitSubscriptingRef.md) (to determine if old value fetching is needed)

## Notes and Other Information
- Returns true if the expression needs the old value passed via CaseTestExpr
- Handles FieldStore expressions by checking if their arg is a CaseTestExpr
- Handles SubscriptingRef expressions by checking if their refexpr is a CaseTestExpr  
- Recursively processes CoerceToDomain and RelabelType to look through type coercions
- CaseTestExpr typically appears directly at the top level rather than deeply nested
- Essential for optimizing assignment operations by avoiding unnecessary old value fetches