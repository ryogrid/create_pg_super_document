# finalize_primnode

## Location
[src/backend/optimizer/plan/subselect.c:2890-2973](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L2890-L2973)

## Overview
Recursively traverses an expression tree to identify and collect all PARAM_EXEC parameter IDs that appear or will appear during plan execution.

## Definition
```c
static bool finalize_primnode(Node *node, finalize_primnode_context *context)
```

## Detailed Description
finalize_primnode is a specialized tree-walking function that processes individual expression nodes to identify parameter dependencies. It serves as the expression-level counterpart to finalize_plan, focusing on collecting PARAM_EXEC parameter IDs from various expression constructs.

The function handles several important node types with special processing:

1. **Param nodes**: Directly extracts PARAM_EXEC parameter IDs and adds them to the context's parameter set.

2. **Aggref nodes**: Handles a special case where aggregate functions may be replaced by parameters during setrefs.c processing. It checks for min/max aggregate replacements and accounts for the replacement parameter if found.

3. **SubPlan nodes**: Processes subquery expressions with careful parameter management:
   - Recursively processes the testexpr
   - Removes output parameter IDs that were referenced in testexpr (since subplans are always re-evaluated)
   - Processes the arguments list
   - Adds external parameters needed by the subplan, excluding those passed down to it

For all other node types, the function delegates to expression_tree_walker for standard recursive traversal.

## Parameters / Member Variables
- `node`: The expression node to be processed (can be NULL)
- `context`: finalize_primnode_context structure containing:
  - root: PlannerInfo for accessing global planning information
  - paramids: Bitmapset accumulating all discovered parameter IDs

## Dependencies
- Functions called/Symbols referenced:
  - [bms_add_member](../b/bms_add_member.md), bms_del_member, bms_copy, bms_join (bitmap set operations)
  - [find_minmax_agg_replacement_param](find_minmax_agg_replacement_param.md) (checks for aggregate replacements)
  - planner_subplan_get_plan (retrieves subplan details)
  - expression_tree_walker (general expression traversal)
  - [Node](../N/Node.md) type checking macros (IsA)
- Called from (representative examples):
  - [finalize_plan](finalize_plan.md) (for processing plan node expressions)
  - [finalize_primnode](finalize_primnode.md) (recursive calls)
  - [finalize_agg_primnode](finalize_agg_primnode.md) (for aggregate-specific processing)

## Notes and Other Information
- Returns false to continue tree walking (standard expression_tree_walker protocol)
- The function assumes SS_finalize_plan has been run on any referenced subplans
- Special handling for aggregate replacement parameters addresses timing issues between planning phases
- Parameter ID management for SubPlan nodes ensures correct dependency tracking while avoiding redundant dependencies
- Critical for identifying all external parameter dependencies in expression trees
- Part of PostgreSQL's parameter finalization subsystem in subselect processing
- Located in src/backend/optimizer/plan/subselect.c (static function)

## Simplified Source

```c
static bool
finalize_primnode(Node *node, finalize_primnode_context *context)
{
    if (node == NULL)
        return false;

    // Handle PARAM_EXEC parameters
    if (IsA(node, Param))
    {
        if (((Param *) node)->paramkind == PARAM_EXEC)
        {
            int paramid = ((Param *) node)->paramid;
            context->paramids = bms_add_member(context->paramids, paramid);
        }
        return false;
    }

    // Handle aggregate functions that may become parameters
    else if (IsA(node, Aggref))
    {
        Aggref *aggref = (Aggref *) node;

        // Check if this aggregate will be replaced by a parameter
        Param *aggparam = find_minmax_agg_replacement_param(context->root, aggref);
        if (aggparam != NULL)
            context->paramids = bms_add_member(context->paramids, aggparam->paramid);

        // Continue to examine aggregate arguments
    }

    // Handle subplan nodes
    else if (IsA(node, SubPlan))
    {
        SubPlan *subplan = (SubPlan *) node;
        Plan *plan = planner_subplan_get_plan(context->root, subplan);

        // Process test expression
        finalize_primnode(subplan->testexpr, context);

        // Remove subplan output parameters from our set
        foreach(lc, subplan->paramIds)
        {
            context->paramids = bms_del_member(context->paramids, lfirst_int(lc));
        }

        // Process subplan arguments
        finalize_primnode((Node *) subplan->args, context);

        // Add external parameters needed by subplan
        Bitmapset *subparamids = bms_copy(plan->extParam);
        foreach(lc, subplan->parParam)
        {
            subparamids = bms_del_member(subparamids, lfirst_int(lc));
        }
        context->paramids = bms_join(context->paramids, subparamids);

        return false;
    }

    // Continue tree traversal for other node types
    return expression_tree_walker(node, finalize_primnode, context);
}
```