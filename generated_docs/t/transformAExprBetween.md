# transformAExprBetween

## Location
[src/backend/parser/parse_expr.c:1284-1377](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L1284-L1377)

## Overview
Transforms A_Expr nodes representing BETWEEN, NOT BETWEEN, and their symmetric variants into equivalent boolean expression trees using comparison operators.

## Definition
```c
static Node *transformAExprBetween(ParseState *pstate, A_Expr *a)
```

## Detailed Description
This function handles the transformation of SQL BETWEEN expressions and their variants during expression parsing. It converts high-level BETWEEN constructs into equivalent boolean logic using basic comparison operators.

The function supports four different BETWEEN variants:

**1. AEXPR_BETWEEN** (expr BETWEEN low AND high):
   - Converts to: (expr >= low) AND (expr <= high)
   - Uses AND logic to ensure the value falls within the range

**2. AEXPR_NOT_BETWEEN** (expr NOT BETWEEN low AND high):
   - Converts to: (expr < low) OR (expr > high)  
   - Uses OR logic to ensure the value falls outside the range

**3. AEXPR_BETWEEN_SYM** (expr BETWEEN SYMMETRIC low AND high):
   - Handles cases where low and high might be in any order
   - Converts to: ((expr >= low) AND (expr <= high)) OR ((expr >= high) AND (expr <= low))
   - Allows for range checking regardless of which bound is actually smaller

**4. AEXPR_NOT_BETWEEN_SYM** (expr NOT BETWEEN SYMMETRIC low AND high):
   - Negation of symmetric between
   - Converts to: ((expr < low) OR (expr > high)) AND ((expr < high) OR (expr > low))
   - Excludes values within either possible range ordering

The transformation process:
1. Extracts the three expressions (test_expr, low_bound, high_bound) from the A_Expr
2. Uses copyObject to safely duplicate expressions that are referenced multiple times
3. Creates new A_Expr nodes for individual comparisons using makeSimpleA_Expr
4. Combines comparisons with appropriate boolean operators (AND/OR)
5. Recursively transforms the resulting expression tree

## Parameters / Member Variables
- `pstate`: ParseState context containing parsing state and environment information
- `a`: A_Expr node representing the BETWEEN expression to transform

## Dependencies
- Functions called/Symbols referenced:
  - castNode
  - linitial
  - lsecond
  - [makeSimpleA_Expr](../m/makeSimpleA_Expr.md)
  - copyObject
  - [makeBoolExpr](../m/makeBoolExpr.md)
  - list_make2
  - [transformExprRecurse](transformExprRecurse.md)
  - elog (for error handling)
- Called from (representative examples):
  - [transformExprRecurse](transformExprRecurse.md)

## Notes and Other Information
- The function is static, meaning it's only accessible within parse_expr.c
- Uses hard-wired comparison operators (>=, <=, <, >) rather than opclasses due to complexity of mixed data types
- The comment notes a limitation: copyObject causes multiple runtime evaluations of potentially volatile expressions
- SYMMETRIC variants handle cases where the user doesn't know which bound is larger
- All transformations preserve the original expression's location information for error reporting
- The final result is recursively transformed to handle any nested expressions
- Located in src/backend/parser/parse_expr.c:1284-1377