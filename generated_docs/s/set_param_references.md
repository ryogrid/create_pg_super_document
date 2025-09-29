# set_param_references

## Location
[src/backend/optimizer/plan/setrefs.c:2497-2551](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L2497-L2551)

## Overview
Initializes the initParam list in Gather or GatherMerge nodes to contain references to all parameters that need to be evaluated before execution of the node.

## Definition

```c
static void
set_param_references(PlannerInfo *root, Plan *plan)
```
## Detailed Description
This function is responsible for setting up parameter references in parallel execution nodes (Gather and GatherMerge). It identifies all initplan parameters that are being passed to the plan nodes below the parallel execution boundary and stores them in the node's initParam field. This ensures that worker processes in parallel execution have access to all necessary parameters that were computed by initplans in the main process.

The function walks up the planner hierarchy to collect all initplan parameters from the current and parent query levels, then intersects this set with the external parameters required by the subtree to determine which parameters need to be passed down to parallel workers.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information
- : The Plan node (must be either Gather or GatherMerge) that needs parameter initialization

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro for type checking)
  - [bms_add_member](../b/bms_add_member.md)
  - lfirst_int
  - [bms_intersect](../b/bms_intersect.md)
- Called from (representative examples):
  - fix_scan_list (src/backend/optimizer/plan/setrefs.c:165)
  - [set_plan_refs](set_plan_refs.md) (src/backend/optimizer/plan/setrefs.c:855)

## Notes and Other Information
- Only applies to Gather and GatherMerge nodes, which are the entry points for parallel execution
- The function asserts that the input plan is either a Gather or GatherMerge node
- Parameter collection traverses the entire planner hierarchy (root and all parent_root levels)
- The initParam field stores the intersection of external parameters needed by the subtree and parameters available from initplans
- This mechanism is crucial for proper parameter passing in parallel query execution

## Simplified Source

```c
static void
set_param_references(PlannerInfo *root, Plan *plan) {
    Assert(IsA(plan, Gather) || IsA(plan, GatherMerge));

    if (plan->lefttree->extParam) {
        Bitmapset *initSetParam = NULL;
        PlannerInfo *proot;

        // Collect all initplan parameters from current and parent query levels
        for (proot = root; proot != NULL; proot = proot->parent_root) {
            foreach(l, proot->init_plans) {
                SubPlan *initsubplan = (SubPlan *) lfirst(l);
                foreach(l2, initsubplan->setParam) {
                    initSetParam = bms_add_member(initSetParam, lfirst_int(l2));
                }
            }
        }

        // Store intersection of external params needed and initplan params available
        if (IsA(plan, Gather)) {
            ((Gather *) plan)->initParam =
                bms_intersect(plan->lefttree->extParam, initSetParam);
        } else {
            ((GatherMerge *) plan)->initParam =
                bms_intersect(plan->lefttree->extParam, initSetParam);
        }
    }
}
```