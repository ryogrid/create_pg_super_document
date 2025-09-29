# finalize_plan

## Location
[src/backend/optimizer/plan/subselect.c:2292-2889](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L2292-L2889)

## Overview
Recursively processes all nodes in a plan tree to compute external parameter dependencies (extParam) and all parameter dependencies (allParam) for each plan node.

## Definition
```c
static Bitmapset *finalize_plan(PlannerInfo *root, Plan *plan, int gather_param, Bitmapset *valid_params, Bitmapset *scan_params)
```

## Detailed Description
finalize_plan is the core recursive function that performs parameter finalization for PostgreSQL plan trees. It traverses the entire plan tree depth-first, computing parameter dependency information that is essential for proper plan execution, particularly in the context of subqueries and correlated queries.

The function computes two critical parameter sets for each plan node:
- extParam: Parameters that come from outside the current plan node (external dependencies)
- allParam: All parameters that the plan node and its entire subtree depend on

The function handles various plan node types with type-specific processing, including scan nodes, join nodes, aggregate nodes, and utility nodes. For each node type, it analyzes the node's expressions and child plans to determine parameter dependencies.

Special handling is provided for:
- InitPlans: Processes initialization plans to determine external and set parameters
- Parallel processing: Handles parallel-aware nodes and Gather/GatherMerge coordination
- Nested loops: Manages parameter passing between left and right child nodes
- Subqueries: Recursively processes subquery plans with proper parameter scoping
- EvalPlanQual: Supports EPQ mechanism through scan_params

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planning context and global information
- `plan`: The Plan node to be processed (can be NULL)
- `gather_param`: Parameter ID from an ancestral Gather/GatherMerge node, or -1 if none
- `valid_params`: Set of parameter IDs that are valid to reference from outer plan levels
- `scan_params`: Set of parameter IDs to force scan nodes to reference (for EvalPlanQual support)

## Dependencies
- Functions called/Symbols referenced:
  - [finalize_primnode](finalize_primnode.md) (processes individual expressions)
  - [finalize_agg_primnode](finalize_agg_primnode.md) (processes aggregate expressions)
  - planner_subplan_get_plan
  - [find_base_rel](find_base_rel.md)
  - Various bitmap set manipulation functions (bms_add_members, bms_union, etc.)
  - [Node](../N/Node.md) type checking (nodeTag)
- Called from (representative examples):
  - [SS_finalize_plan](../S/SS_finalize_plan.md) (entry point)
  - [finalize_plan](finalize_plan.md) (recursive calls to child plans)

## Notes and Other Information
- Returns the computed allParam set for the given plan node
- The function is designed to handle all plan node types through a comprehensive switch statement
- Parameter validation ensures that plan nodes only reference parameters that are valid in their scope
- [InitPlan](../I/InitPlan.md) processing assumes SS_finalize_plan has already been run on referenced plans
- The function includes extensive comments about limitations in initPlan parameter handling
- Critical for proper execution of correlated subqueries and nested plan structures
- Located in src/backend/optimizer/plan/subselect.c (static function)

## Simplified Source

```c
static Bitmapset *finalize_plan(PlannerInfo *root, Plan *plan,
                               int gather_param,
                               Bitmapset *valid_params,
                               Bitmapset *scan_params) {
    finalize_primnode_context context;
    int locally_added_param = -1;
    Bitmapset *nestloop_params = NULL;

    if (plan == NULL)
        return NULL;

    // Initialize context for parameter collection
    context.root = root;
    context.paramids = NULL;

    // Process initPlans to get external and set parameters
    Bitmapset *initExtParam = NULL, *initSetParam = NULL;
    foreach(l, plan->initPlan) {
        SubPlan *initsubplan = (SubPlan *) lfirst(l);
        Plan *initplan = planner_subplan_get_plan(root, initsubplan);
        initExtParam = bms_add_members(initExtParam, initplan->extParam);
        // Add setParam IDs to initSetParam
        foreach(l2, initsubplan->setParam) {
            initSetParam = bms_add_member(initSetParam, lfirst_int(l2));
        }
    }

    // Update valid_params with any setParams from initPlans
    if (initSetParam)
        valid_params = bms_union(valid_params, initSetParam);

    // Process targetlist and qual expressions
    finalize_primnode((Node *) plan->targetlist, &context);
    finalize_primnode((Node *) plan->qual, &context);

    // Handle parallel-aware nodes
    if (plan->parallel_aware) {
        if (gather_param < 0)
            elog(ERROR, "parallel-aware plan node is not below a Gather");
        context.paramids = bms_add_member(context.paramids, gather_param);
    }

    // Node-type-specific processing (simplified for key cases)
    switch (nodeTag(plan)) {
        case T_SeqScan:
        case T_IndexScan:
        case T_BitmapHeapScan:
            context.paramids = bms_add_members(context.paramids, scan_params);
            break;

        case T_SubqueryScan: {
            // Recursively process subquery
            SubqueryScan *sscan = (SubqueryScan *) plan;
            RelOptInfo *rel = find_base_rel(root, sscan->scan.scanrelid);
            Bitmapset *subquery_params = rel->subroot->outer_params;
            if (gather_param >= 0)
                subquery_params = bms_add_member(bms_copy(subquery_params), gather_param);
            finalize_plan(rel->subroot, sscan->subplan, gather_param, subquery_params, NULL);
            context.paramids = bms_add_members(context.paramids, sscan->subplan->extParam);
            context.paramids = bms_add_members(context.paramids, scan_params);
            break;
        }

        case T_NestLoop: {
            // Handle nestloop parameters
            finalize_primnode((Node *) ((Join *) plan)->joinqual, &context);
            foreach(l, ((NestLoop *) plan)->nestParams) {
                NestLoopParam *nlp = (NestLoopParam *) lfirst(l);
                nestloop_params = bms_add_member(nestloop_params, nlp->paramno);
            }
            break;
        }

        case T_ModifyTable: {
            // Add EPQ parameter for EvalPlanQual support
            ModifyTable *mtplan = (ModifyTable *) plan;
            locally_added_param = mtplan->epqParam;
            valid_params = bms_add_member(bms_copy(valid_params), locally_added_param);
            scan_params = bms_add_member(bms_copy(scan_params), locally_added_param);
            break;
        }

        // ... other node types would be handled similarly
        default:
            // Most node types don't need special processing
            break;
    }

    // Process child plans
    Bitmapset *child_params;
    child_params = finalize_plan(root, plan->lefttree, gather_param, valid_params, scan_params);
    context.paramids = bms_add_members(context.paramids, child_params);

    // Handle right child - special case for nestloop parameters
    if (nestloop_params) {
        child_params = finalize_plan(root, plan->righttree, gather_param,
                                   bms_union(nestloop_params, valid_params), scan_params);
        child_params = bms_difference(child_params, nestloop_params);
        bms_free(nestloop_params);
    } else {
        child_params = finalize_plan(root, plan->righttree, gather_param, valid_params, scan_params);
    }
    context.paramids = bms_add_members(context.paramids, child_params);

    // Remove locally generated parameters from external dependencies
    if (locally_added_param >= 0) {
        context.paramids = bms_del_member(context.paramids, locally_added_param);
    }

    // Validate parameter scope
    if (!bms_is_subset(context.paramids, valid_params))
        elog(ERROR, "plan should not reference subplan's variable");

    // Set final parameter fields
    plan->allParam = bms_union(context.paramids, initExtParam);
    plan->allParam = bms_add_members(plan->allParam, initSetParam);
    plan->extParam = bms_union(context.paramids, initExtParam);
    plan->extParam = bms_del_members(plan->extParam, initSetParam);

    return plan->allParam;
}
```