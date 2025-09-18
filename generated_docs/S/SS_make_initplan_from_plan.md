# SS_make_initplan_from_plan

## Location
src/backend/optimizer/plan/subselect.c: 3017 - 3057

## Overview
Converts a given plan tree into an InitPlan by creating a SubPlan node and adding it to the outer query's initplan list.

## Definition
```c
void SS_make_initplan_from_plan(PlannerInfo *root,
                               PlannerInfo *subroot, Plan *plan,
                               Param *prm)
```

## Detailed Description
This function takes a completed plan tree and converts it into an initialization plan (initplan) that will be executed once before the main query execution begins. The function creates a SubPlan node of type EXPR_SUBLINK, registers the plan with the global planner state, and adds it to the initplan list. The initplan's output will be stored in the provided Param node, which was previously created using `SS_make_initplan_output_param`.

The function performs several key operations:
1. Adds the subplan, its PlannerInfo, and a dummy path to the global lists
2. Creates a SubPlan node with appropriate metadata
3. Extracts type information from the plan's first target list entry
4. Sets up the parameter binding for the initplan's output
5. Calculates and assigns the cost of the subplan

## Parameters / Member Variables
- `root`: Main query's PlannerInfo structure where the initplan will be added
- `subroot`: PlannerInfo structure for the subquery being converted to an initplan
- `plan`: The plan tree to be converted into an initplan
- `prm`: Param node that will receive the initplan's output value (created by SS_make_initplan_output_param)

## Dependencies
- Functions called/Symbols referenced:
  - `lappend`: List manipulation function to add elements to global lists
  - `makeNode`: Creates a new SubPlan node
  - `[psprintf](../p/psprintf.md)`: Formatted string printing function
  - `[get_first_col_type](../g/get_first_col_type.md)`: Extracts type information from the plan's first column
  - `list_make1_int`: Creates a single-element integer list
  - `[cost_subplan](../c/cost_subplan.md)`: Calculates the cost of executing the subplan
  - `EXPR_SUBLINK`: SubLink type constant for expression sublinks
- Called from (representative examples):
  - `[create_minmaxagg_plan](../c/create_minmaxagg_plan.md)`: Used when creating initplans for MIN/MAX aggregate optimization
  - Referenced in `src/include/optimizer/subselect.h`: Function prototype declaration

## Notes and Other Information
- The function adds a dummy (NULL) path entry to the global subpaths list because the exact path matching the plan is not constructed by current callers
- The SubPlan is added to the initplan list in the correct order, respecting dependencies (later initplans may depend on earlier ones)
- The initplan has no input parameters (parParam and args lists remain empty) since it's executed independently
- The parallel_safe flag is inherited from the source plan to maintain parallel execution safety
- The plan_name is automatically generated as "InitPlan N" where N is the plan ID
- Location: `src/backend/optimizer/plan/subselect.c:3017-3057`
- This function works in conjunction with `SS_make_initplan_output_param` to provide a complete initplan creation workflow