# get_expr_width

## Location
[src/backend/optimizer/path/costsize.c:6297-6344](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L6297-L6344)

## Overview
Estimates the width of a given expression by attempting to use cached width data from RelOptInfo for Vars, or falling back to datatype-based width estimates for other node types.

## Definition
```c
static int32 get_expr_width(PlannerInfo *root, const Node *expr)
```

## Detailed Description
This function provides width estimates for arbitrary expressions in an efficient manner by leveraging cached data when possible. For Var nodes (column references), it attempts to retrieve previously cached width estimates from the owning relation's RelOptInfo structure, which may have been populated by earlier calls to `set_rel_width` or similar functions. The cache lookup involves validating that the Var references a normal relation (not a special varno), that the relation exists in the simple_rel_array, and that the attribute number falls within the cached range.

When cached data is unavailable or the expression is not a Var, the function falls back to datatype-based width estimates using `get_typavgwidth`. This fallback approach provides reasonable estimates based on the PostgreSQL type system without requiring expensive catalog lookups or statistical analysis.

The function is designed to be lightweight and efficient, making it suitable for use in cost estimation routines that may be called frequently during query planning.

## Parameters / Member Variables
- `root`: Pointer to PlannerInfo structure containing global planning information and relation cache
- `expr`: Pointer to the Node (expression) whose width is being estimated

## Dependencies
- Functions called/Symbols referenced:
  - [get_typavgwidth](get_typavgwidth.md)
  - [exprType](../e/exprType.md)
  - [exprTypmod](../e/exprTypmod.md)
- Macros used:
  - IS_SPECIAL_VARNO
- Called from (representative examples):
  - [set_pathtarget_cost_width](../s/set_pathtarget_cost_width.md)
  - [cost_memoize_rescan](../c/cost_memoize_rescan.md)

## Notes and Other Information
- Optimizes for performance by checking cached width estimates in RelOptInfo before falling back to type-based estimates
- Only works with upper-level Vars (varlevelsup == 0) as validated by assertion
- Validates that Var references fall within the cached attribute range of the relation
- Always returns a positive width value (validated by assertions)
- Provides a uniform interface for width estimation across different expression types
- Essential building block for PostgreSQL's cost estimation and query optimization infrastructure

## Simplified Source

```c
static int32 get_expr_width(PlannerInfo *root, const Node *expr) {
    int32 width;

    // If expression is a Var (column reference)
    if (IsA(expr, Var)) {
        const Var *var = (const Var *) expr;

        // Try to get cached width from RelOptInfo
        if (!IS_SPECIAL_VARNO(var->varno) &&
            var->varno < root->simple_rel_array_size) {

            RelOptInfo *rel = root->simple_rel_array[var->varno];

            // Check if we have cached width data for this attribute
            if (rel != NULL &&
                var->varattno >= rel->min_attr &&
                var->varattno <= rel->max_attr) {

                int ndx = var->varattno - rel->min_attr;

                // Return cached width if available
                if (rel->attr_widths[ndx] > 0)
                    return rel->attr_widths[ndx];
            }
        }

        // No cached data - use type-based estimate
        width = get_typavgwidth(var->vartype, var->vartypmod);
        return width;
    }

    // For non-Var expressions, use type-based estimate
    width = get_typavgwidth(exprType(expr), exprTypmod(expr));
    return width;
}
```