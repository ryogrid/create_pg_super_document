# add_nullingrels_if_needed

## Location
[src/backend/optimizer/util/var.c:910-961](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/var.c#L910-L961)

## Overview
Adds varnullingrels information from an original Var to a flattened join alias expression, ensuring NULL-producing join semantics are preserved.

## Definition
```c
static Node *add_nullingrels_if_needed(PlannerInfo *root, Node *newnode, Var *oldvar)
```

## Detailed Description
This function is responsible for preserving NULL-producing join semantics when flattening join alias variables. When a Var that references a JOIN output is replaced with its underlying expression, any varnullingrels information (indicating which outer joins might make this Var NULL) must be transferred to the replacement expression.

The function employs a two-tier strategy:

**Tier 1 - Direct Integration**: If the replacement expression is a "standard" join alias expression (as determined by is_standard_join_alias_expression), the function can directly add the nullingrels to existing nullingrels fields in Vars and PlaceHolderVars within the expression using adjust_standard_join_alias_expression.

**Tier 2 - PlaceHolderVar Wrapper**: For more complex expressions that cannot accommodate direct nullingrels integration, the function wraps the entire expression in a PlaceHolderVar. The PlaceHolderVar carries the nullingrels information and is evaluated at an appropriate query level.

The evaluation placement logic for PlaceHolderVars is sophisticated:
- First attempts to evaluate at the natural semantic level of the new expression
- For variable-free expressions, falls back to evaluating at the join level of the original Var
- Ensures proper handling of outer joins by evaluating below rather than above the join

## Parameters / Member Variables
- `root`: PlannerInfo structure; NULL when called from parser (prevents PlaceHolderVar creation)
- `newnode`: The flattened replacement expression (already copied, can be modified)
- `oldvar`: The original Var being replaced, containing varnullingrels to be preserved

## Dependencies
- Functions called/Symbols referenced:
  - [is_standard_join_alias_expression](../i/is_standard_join_alias_expression.md)
  - [adjust_standard_join_alias_expression](adjust_standard_join_alias_expression.md)
  - [pull_varnos_of_level](../p/pull_varnos_of_level.md)
  - bms_is_empty, bms_del_member, bms_copy
  - [get_relids_for_join](../g/get_relids_for_join.md)
  - [make_placeholder_expr](../m/make_placeholder_expr.md)
- Called from (representative examples):
  - [flatten_join_alias_vars_mutator](../f/flatten_join_alias_vars_mutator.md) (for both regular and whole-row Var replacements)

## Notes and Other Information
- Returns the original newnode unchanged if oldvar has no varnullingrels
- The function will ERROR if called from the parser (root == NULL) with a non-standard expression, indicating missing parser support
- [PlaceHolderVar](../P/PlaceHolderVar.md) creation includes proper level adjustment (phlevelsup) and nullingrels copying (phnullingrels)
- [Variable](../V/Variable.md)-free expressions require special handling to determine appropriate evaluation placement
- The function assumes that standard join alias expressions can always accommodate direct nullingrels integration

## Simplified Source

```c
static Node *
add_nullingrels_if_needed(PlannerInfo *root, Node *newnode, Var *oldvar)
{
    // Nothing to do if oldvar has no nullingrels
    if (oldvar->varnullingrels == NULL)
        return newnode;

    // Try direct integration for standard join alias expressions
    if (is_standard_join_alias_expression(newnode, oldvar)) {
        adjust_standard_join_alias_expression(newnode, oldvar);
    }
    // Use PlaceHolderVar wrapper for complex expressions
    else if (root) {
        PlaceHolderVar *newphv;
        Index levelsup = oldvar->varlevelsup;
        Relids phrels = pull_varnos_of_level(root, newnode, levelsup);

        // For variable-free expressions, evaluate at join level
        if (bms_is_empty(phrels)) {
            if (levelsup != 0)
                elog(ERROR, "unsupported join alias expression");
            phrels = get_relids_for_join(root->parse, oldvar->varno);
            // Evaluate below outer join, not above
            phrels = bms_del_member(phrels, oldvar->varno);
            Assert(!bms_is_empty(phrels));
        }

        // Create PlaceHolderVar with proper nullingrels
        newphv = make_placeholder_expr(root, (Expr *) newnode, phrels);
        newphv->phlevelsup = levelsup;
        newphv->phnullingrels = bms_copy(oldvar->varnullingrels);
        newnode = (Node *) newphv;
    }
    else {
        // Parser context - missing support
        elog(ERROR, "unsupported join alias expression");
    }

    return newnode;
}
```