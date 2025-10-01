# pullup_replace_vars_callback

## Location
[src/backend/optimizer/prep/prepjointree.c:2484-2786](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L2484-L2786)

## Overview
The callback function used by replace_rte_variables to perform actual variable substitution during subquery pullup, handling complex cases like whole-tuple references and PlaceHolderVar wrapping.

## Definition
```c
static Node *pullup_replace_vars_callback(Var *var,
                                         replace_rte_variables_context *context)
```

## Detailed Description
This function performs the actual variable replacement logic for subquery pullup operations. It is called by the generic `replace_rte_variables` function for each variable that references the target subquery being pulled up.

The function handles several complex scenarios:
1. **PlaceHolderVar wrapping**: Determines when replacement expressions need to be wrapped in PlaceHolderVars to handle outer join semantics correctly
2. **Whole-tuple references**: Expands references to entire tuples (`varattno = 0`) into RowExpr constructs
3. **Normal attribute references**: Substitutes individual column references with expressions from the subquery's target list
4. **Caching**: Maintains a cache of wrapped expressions to avoid creating duplicate PlaceHolderVars with different IDs
5. **Nulling relations**: Propagates nulling relations from the original variable to the replacement expression
6. **LATERAL references**: Handles special cases for LATERAL subqueries with complex nulling relation management

The function uses sophisticated logic to determine whether expressions need PlaceHolderVar wrapping, including analysis of strictness and variable membership to optimize the generated plan.

## Parameters / Member Variables
- `var`: The Var node being replaced (references the pulled-up subquery)
- `context`: Generic replacement context containing callback arguments and sublevels information

## Dependencies
- Functions called/Symbols referenced:
  - copyObject, get_tle_by_resno, expandRTE
  - [make_placeholder_expr](../m/make_placeholder_expr.md), add_nulling_relids
  - [pull_varnos](pull_varnos.md), contain_vars_of_level, contain_nonstrict_functions
  - bms_* functions (bitmap set operations)
  - [IncrementVarSublevelsUp](../I/IncrementVarSublevelsUp.md), replace_rte_variables_mutator
  - RowExpr, PlaceHolderVar, InvalidAttrNumber
  - COERCE_IMPLICIT_CAST, nullingrel_info
- Called from (representative examples):
  - [pullup_replace_vars](pullup_replace_vars.md) (via replace_rte_variables)
  - [pullup_replace_vars_subquery](pullup_replace_vars_subquery.md) (via replace_rte_variables)

## Notes and Other Information
- Uses caching in `rcon->rv_cache[]` to avoid creating duplicate PlaceHolderVars with different IDs
- Handles whole-tuple expansion by creating RowExpr with proper column handling for named vs RECORD types
- For LATERAL subqueries, implements complex logic to propagate nulling relations correctly to lateral references
- Optimizes PlaceHolderVar usage by analyzing expression strictness and variable membership
- Preserves nulling relations from original variables and propagates them appropriately in replacement expressions
- Handles varlevelsup adjustments when the replaced variable is within nested subqueries

## Simplified Source

```c
static Node *
pullup_replace_vars_callback(Var *var, replace_rte_variables_context *context)
{
    pullup_replace_vars_context *rcon = (pullup_replace_vars_context *) context->callback_arg;
    int varattno = var->varattno;
    Node *newnode;

    // Determine if PlaceHolderVar wrapping is needed
    bool need_phv = (var->varnullingrels != NULL) || rcon->wrap_non_vars;

    // Check cache first if PHV needed
    if (need_phv && varattno >= InvalidAttrNumber &&
        varattno <= list_length(rcon->targetlist) &&
        rcon->rv_cache[varattno] != NULL) {
        newnode = copyObject(rcon->rv_cache[varattno]);
    }
    else if (varattno == InvalidAttrNumber) {
        // Whole-tuple reference: expand into RowExpr
        List *colnames, *fields;
        expandRTE(rcon->target_rte, var->varno, 0, var->location,
                  (var->vartype != RECORDOID), &colnames, &fields);

        // Process the expanded fields
        fields = (List *) replace_rte_variables_mutator((Node *) fields, context);

        RowExpr *rowexpr = makeNode(RowExpr);
        rowexpr->args = fields;
        rowexpr->row_typeid = var->vartype;
        rowexpr->row_format = COERCE_IMPLICIT_CAST;
        rowexpr->colnames = (var->vartype == RECORDOID) ? colnames : NIL;
        rowexpr->location = var->location;
        newnode = (Node *) rowexpr;

        // Wrap in PlaceHolderVar if needed
        if (need_phv) {
            newnode = (Node *) make_placeholder_expr(rcon->root, (Expr *) newnode,
                                                   bms_make_singleton(rcon->varno));
            rcon->rv_cache[InvalidAttrNumber] = copyObject(newnode);
        }
    }
    else {
        // Normal attribute reference
        TargetEntry *tle = get_tle_by_resno(rcon->targetlist, varattno);
        if (tle == NULL)
            elog(ERROR, "could not find attribute %d in subquery targetlist", varattno);

        newnode = (Node *) copyObject(tle->expr);

        // Decide whether to wrap in PlaceHolderVar
        if (need_phv) {
            bool wrap = true;

            // Simple Vars and PHVs may not need wrapping in some cases
            if (IsA(newnode, Var) || IsA(newnode, PlaceHolderVar)) {
                // Check lateral reference conditions
                if (rcon->target_rte->lateral) {
                    // Complex lateral reference logic...
                    wrap = true; // Simplified
                } else {
                    wrap = false;
                }
            } else if (!rcon->wrap_non_vars) {
                // Check if expression contains vars and is strict
                if (contain_vars_of_level(newnode, 0) &&
                    !contain_nonstrict_functions(newnode)) {
                    wrap = false;
                }
            }

            if (wrap) {
                newnode = (Node *) make_placeholder_expr(rcon->root, (Expr *) newnode,
                                                       bms_make_singleton(rcon->varno));
                if (varattno > InvalidAttrNumber && varattno <= list_length(rcon->targetlist))
                    rcon->rv_cache[varattno] = copyObject(newnode);
            }
        }
    }

    // Propagate nulling relations
    if (var->varnullingrels != NULL) {
        if (IsA(newnode, Var)) {
            ((Var *) newnode)->varnullingrels =
                bms_add_members(((Var *) newnode)->varnullingrels, var->varnullingrels);
        } else if (IsA(newnode, PlaceHolderVar)) {
            ((PlaceHolderVar *) newnode)->phnullingrels =
                bms_add_members(((PlaceHolderVar *) newnode)->phnullingrels, var->varnullingrels);
        } else {
            // Add nulling relations to contained vars
            newnode = add_nulling_relids(newnode, rcon->relids, var->varnullingrels);
        }
    }

    // Adjust variable levels if needed
    if (var->varlevelsup > 0)
        IncrementVarSublevelsUp(newnode, var->varlevelsup, 0);

    return newnode;
}
```