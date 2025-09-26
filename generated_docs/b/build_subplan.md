# build_subplan

## Location
[src/backend/optimizer/plan/subselect.c:319-579](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/subselect.c#L319-L579)

## Overview
Constructs a SubPlan node from raw planning inputs, determining whether to implement it as a regular SubPlan or InitPlan and handling parameter setup and optimization decisions.

## Definition
```c
static Node *build_subplan(PlannerInfo *root, Plan *plan, Path *path, PlannerInfo *subroot, List *plan_params, SubLinkType subLinkType, int subLinkId, Node *testexpr, List *testexpr_paramids, bool unknownEqFalse)
```

## Detailed Description
This function serves as a subroutine for make_subplan, constructing the actual SubPlan node from the planned subquery. It makes critical decisions about execution strategy based on the sublink type and correlation patterns:

1. **InitPlan Creation**: For uncorrelated subqueries of types EXISTS, EXPR, ARRAY, ROWCOMPARE, or MULTIEXPR, creates InitPlans that execute once and store results in parameters.

2. **Parameter Management**: Sets up parameter passing between outer and inner queries, handling both parParam (parameters passed to subplan) and setParam (parameters set by subplan).

3. **Optimization Decisions**: Determines whether to use hash tables for ANY sublinks, add materialization for repeated access, or enable special handling for different sublink types.

4. **Global Registration**: Registers the subplan, path, and planner info in global lists for later reference during execution.

The function returns either the SubPlan node itself (for regular subplans) or a replacement expression like a Param node (for InitPlans).

## Parameters / Member Variables
- `root`: PlannerInfo context for the outer query
- `plan`: The planned subquery to be wrapped in a SubPlan
- `path`: The path that generated the plan
- `subroot`: PlannerInfo context used for planning the subquery
- `plan_params`: List of parameters needed by the subplan
- `subLinkType`: Type of the original SubLink (EXISTS, ANY, ALL, etc.)
- `subLinkId`: Unique identifier for the SubLink
- `testexpr`: Test expression for the sublink (may be NULL)
- `testexpr_paramids`: Pre-computed parameter IDs for the test expression
- `unknownEqFalse`: Whether to treat NULL comparisons as FALSE

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [get_first_col_type](../g/get_first_col_type.md)
  - [SS_process_sublinks](../S/SS_process_sublinks.md)
  - [lappend_int](../l/lappend_int.md)
  - [lappend](../l/lappend.md)
  - [generate_new_exec_param](../g/generate_new_exec_param.md)
  - list_make1_int
  - [generate_subquery_params](../g/generate_subquery_params.md)
  - [convert_testexpr](../c/convert_testexpr.md)
  - [list_copy](../l/list_copy.md)
  - [list_nth_cell](../l/list_nth_cell.md)
  - [makeNullConst](../m/makeNullConst.md)
  - [subplan_is_hashable](../s/subplan_is_hashable.md)
  - [testexpr_is_hashable](../t/testexpr_is_hashable.md)
  - [ExecMaterializesOutput](../E/ExecMaterializesOutput.md)
  - [materialize_finished_plan](../m/materialize_finished_plan.md)
  - [bms_add_member](bms_add_member.md)
  - [psprintf](../p/psprintf.md)
  - [cost_subplan](../c/cost_subplan.md)
- Called from (representative examples):
  - [make_subplan](../m/make_subplan.md) (twice - for main plan and alternative hash plan)

## Notes and Other Information
- The function handles five main sublink types with different execution strategies
- InitPlans are preferred for uncorrelated subqueries as they execute only once
- [Hash](../H/Hash.md) tables are used for ANY sublinks when the subplan output and test expression are both hashable
- Materialization is added to uncorrelated subplans to reduce repeated scan costs, unless the plan already materializes output
- MULTIEXPR sublinks require special handling to set multiple PARAM_EXEC parameters
- The function manages the global rewindPlanIDs bitmap to optimize subplan rewinding
- Located in src/backend/optimizer/plan/subselect.c:319-579