# expr_is_nonnullable

## Location
src/backend/optimizer/plan/initsplan.c: 2704 - 2739

## Overview
Determines whether an expression cannot be NULL by checking if it's a simple Var that is NOT NULL and not nulled by outer joins.

## Definition
```c
static bool expr_is_nonnullable(PlannerInfo *root, Expr *expr)
```

## Detailed Description
This function performs a conservative analysis to determine if an expression is guaranteed to be non-null. The analysis is currently limited to simple Var nodes and checks several conditions:

1. **Expression Type Check**: Only handles simple Var nodes, returning false for all other expression types
2. **Outer Join Analysis**: Examines the var's varnullingrels to ensure no outer joins could null the variable
3. **System Column Check**: System columns (varattno < 0) are always considered non-nullable
4. **NOT NULL Constraint Check**: For regular columns, checks if the column has a NOT NULL constraint

The function is used as a helper for optimization decisions, particularly in determining when qualifications involving null checks can be simplified.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global planner state
- `expr`: Expression to analyze for nullability

## Dependencies
- Functions called/Symbols referenced:
  - bms_is_empty
  - [find_base_rel](../f/find_base_rel.md)
  - [bms_is_member](../b/bms_is_member.md)
- Called from (representative examples):
  - [restriction_is_always_true](../r/restriction_is_always_true.md)
  - [restriction_is_always_false](../r/restriction_is_always_false.md)

## Notes and Other Information
**Conservative Approach**: The function takes a conservative approach, only returning true when it can definitively prove non-nullability. Complex expressions, functions, and other node types are assumed to be potentially nullable.

**Outer Join Awareness**: The function is aware of PostgreSQL's outer join nulling semantics. Even if a column is defined NOT NULL, it can still become NULL if it's on the nullable side of an outer join, which is tracked in varnullingrels.

**System Column Handling**: System columns like ctid, xmin, xmax are inherently non-nullable and are handled as a special case.

**Optimization Context**: This function supports query optimization by identifying cases where NULL-related conditions (like IS NULL or IS NOT NULL) can be evaluated at planning time rather than execution time.

**Future Extensions**: The comment 'For now only check simple Vars' suggests that the function could potentially be extended to handle more complex expressions in the future, such as function calls that are known to never return NULL.

The function plays an important role in the constant folding and qual simplification optimizations within PostgreSQL's query planner.