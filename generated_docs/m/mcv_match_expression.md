# mcv_match_expression

## Location
src/backend/statistics/mcv.c: 1535 - 1598

## Overview
Matches an attribute or expression to a dimension of the MCV (Most Common Values) statistic and returns the zero-based index of the matching statistics dimension.

## Definition
```c
static int mcv_match_expression(Node *expr, Bitmapset *keys, List *exprs, Oid *collid)
```

## Detailed Description
This function determines which dimension of a multi-dimensional MCV statistic corresponds to a given expression or variable. It handles two types of inputs: simple column references (Var nodes) and complex expressions. For simple variables, it uses the variable's attribute number to find the matching dimension in the keys bitmap. For expressions, it searches through the expressions list to find an equal expression. The function also optionally determines the collation of the matched expression, which is important for proper comparison operations.

## Parameters / Member Variables
- `expr`: The expression or variable node to match against the statistics dimensions
- `keys`: Bitmapset containing the attribute numbers of simple columns in the statistics
- `exprs`: List of expression nodes for complex expressions in the statistics
- `collid`: Optional output parameter to receive the collation OID of the matched expression

## Dependencies
- Functions called/Symbols referenced:
  - bms_member_index
  - exprCollation
  - bms_num_members
  - equal
- Called from (representative examples):
  - mcv_get_match_bitmap (multiple calls)

## Notes and Other Information
- This is a static function, only accessible within the mcv.c file
- Handles both simple column references (Var nodes) and complex expressions
- Expression dimensions are stored after simple column dimensions in the statistics structure
- Returns -1 or throws an error if the expression is not found in the statistics object
- The collation information is crucial for proper string comparison operations
- Located in src/backend/statistics/mcv.c:1535-1598