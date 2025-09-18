# select_common_type

## Location
src/backend/parser/parse_coerce.c: 1344 - 1479

## Overview
Determines the common supertype from a list of expressions, used for resolving output types in CASE expressions, UNION operations, and similar SQL constructs that require type unification.

## Definition
```c
Oid select_common_type(ParseState *pstate, List *exprs, const char *context,
                      Node **which_expr)
```

## Detailed Description
This function implements PostgreSQL's type resolution algorithm for determining a common supertype when multiple expressions of potentially different types need to be unified. The algorithm follows these steps:

1. **Exact Match Check**: If all expressions have exactly the same type (including domain types), that type is selected immediately.

2. **Base Type Analysis**: For mixed types, the algorithm converts domain types to their base types and examines type categories and preferences.

3. **Category Compatibility**: Types from different categories (e.g., numeric vs string) cannot be unified and result in an error.

4. **Preference-Based Selection**: Within the same category, preferred types are favored, and implicit coercibility is considered when selecting between non-preferred types.

5. **Unknown Type Handling**: If all inputs are UNKNOWN type literals, TEXT is selected as the default resolution.

The function is crucial for SQL constructs like CASE expressions, UNION queries, and array literals where multiple values must be unified to a single type.

## Parameters / Member Variables
- `pstate`: ParseState for error reporting context (may be NULL)
- `exprs`: Non-empty list of expressions to find common type for
- `context`: Description for error messages (e.g., "CASE", "UNION"); NULL to return InvalidOid on failure instead of throwing error
- `which_expr`: Output parameter receiving pointer to the expression from which the result type was selected (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - linitial, list_second_cell, for_each_cell (list manipulation)
  - exprType, exprLocation
  - [getBaseType](../g/getBaseType.md)
  - [get_type_category_preferred](../g/get_type_category_preferred.md)
  - [can_coerce_type](../c/can_coerce_type.md)
  - [format_type_be](../f/format_type_be.md)
  - [parser_errposition](../p/parser_errposition.md)
  - COERCION_IMPLICIT (constant)
  - TYPCATEGORY (type)
- Called from (representative examples):
  - [transformValuesClause](../t/transformValuesClause.md) (src/backend/parser/analyze.c:1592)
  - [transformSetOperationTree](../t/transformSetOperationTree.md) (src/backend/parser/analyze.c:2204)
  - transformCaseExpr (src/backend/parser/parse_expr.c:1731)
  - transformArrayExpr (src/backend/parser/parse_expr.c:2100)
  - transformCoalesceExpr (src/backend/parser/parse_expr.c:2231)

## Notes and Other Information
- Earlier expressions in the list are preferred when there is ambiguity in type selection
- Domain types are preserved only when all expressions have exactly the same domain type; otherwise they are reduced to base types
- The function determines type compatibility but does not guarantee that all inputs can be coerced to the selected type - callers should verify with `verify_common_type`
- Default resolution for all-UNKNOWN input is TEXT type to avoid runtime coercion issues
- Critical component of PostgreSQL's type system for SQL standard compliance in type unification scenarios