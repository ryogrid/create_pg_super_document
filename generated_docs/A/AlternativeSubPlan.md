# AlternativeSubPlan

## Location
[src/include/nodes/primnodes.h:1108-1114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L1108-L1114)

## Overview
AlternativeSubPlan is a transient expression node used during planning to represent a choice among equivalent SubPlans, removed before execution.

## Definition

```c
typedef struct AlternativeSubPlan
{
	pg_node_attr(no_query_jumble)

	Expr		xpr;
	List	   *subplans;		/* SubPlan(s) with equivalent results */
} AlternativeSubPlan;
```
## Detailed Description
AlternativeSubPlan is used only transiently during the planning phase and is completely removed before the plan reaches the executor. It represents a choice among multiple SubPlans that produce equivalent results but may have different performance characteristics.

The subplans are stored as a List to allow for potential future expansion beyond the current implementation. Currently, there are always exactly two alternatives, with the first one being the "fast-start" plan - a plan optimized for quick initial results.

This node allows the planner to defer the choice between different execution strategies until more information is available about the query context and requirements.

## Parameters / Member Variables
- `xpr`: Base Expr node structure
- `*subplans`: List of SubPlan nodes with equivalent results (currently always exactly two, with the first being the fast-start plan)
## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references)
- Called from (representative examples):
  - [make_subplan](../m/make_subplan.md)
  - [process_sublinks_mutator](../p/process_sublinks_mutator.md)
  - [fix_alternative_subplan](../f/fix_alternative_subplan.md)
  - [cost_qual_eval_walker](../c/cost_qual_eval_walker.md)

## Notes and Other Information
- This node exists only during planning and is never seen by the executor
- Always contains exactly two SubPlan alternatives in current implementation
- The first subplan is always the "fast-start" plan optimized for quick initial results
- The List structure allows for potential future expansion to more than two alternatives
- Used to defer execution strategy choice until sufficient context is available