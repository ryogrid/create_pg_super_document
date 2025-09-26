# set_rel_width

## Location
[src/backend/optimizer/path/costsize.c:6102-6258](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/costsize.c#L6102-L6258)

## Overview
Estimates the output width of a base relation by calculating the sum of per-attribute width estimates for referenced columns plus any placeholder variables and expressions, while also computing evaluation costs.

## Definition
```c
static void set_rel_width(PlannerInfo *root, RelOptInfo *rel)
```

## Detailed Description
This function calculates the estimated output width (tuple size) for a base relation by analyzing its target list and computing width estimates for each output column. It processes three types of expressions: regular Vars (table columns), PlaceHolderVars (expressions that need to be evaluated at this level), and general expressions. For regular Vars, it attempts to obtain accurate width estimates from system statistics via `get_attavgwidth`, falling back to datatype-based estimates from `get_typavgwidth` if statistics are unavailable. The function caches per-attribute width estimates for potential reuse during join planning.

The function also handles whole-row references by computing the total width of all columns plus heap tuple header overhead. Additionally, despite its name, it calculates and sets the evaluation cost for the relation's target list, accounting for the computational cost of placeholder variables and complex expressions.

Width estimates are clamped using `clamp_width_est` to prevent integer overflow and ensure reasonable values are maintained throughout planning.

## Parameters / Member Variables
- `root`: Pointer to PlannerInfo structure containing global planning information and context
- `rel`: Pointer to RelOptInfo structure for the relation whose width is being estimated

## Dependencies
- Functions called/Symbols referenced:
  - planner_rt_fetch
  - [get_attavgwidth](../g/get_attavgwidth.md)
  - [get_typavgwidth](../g/get_typavgwidth.md)
  - [find_placeholder_info](../f/find_placeholder_info.md)
  - [cost_qual_eval_node](../c/cost_qual_eval_node.md)
  - [get_relation_data_width](../g/get_relation_data_width.md)
  - [clamp_width_est](../c/clamp_width_est.md)
  - [exprType](../e/exprType.md)
  - [exprTypmod](../e/exprTypmod.md)
- Types used:
  - [PlaceHolderVar](../P/PlaceHolderVar.md)
  - [PlaceHolderInfo](../P/PlaceHolderInfo.md)
  - [QualCost](../Q/QualCost.md)
  - [PathTarget](../P/PathTarget.md)
- Constants used:
  - SizeofHeapTupleHeader
- Called from (representative examples):
  - [set_baserel_size_estimates](set_baserel_size_estimates.md)
  - [set_foreign_size_estimates](set_foreign_size_estimates.md)

## Notes and Other Information
- Works best on plain relations with real Vars; less accurate for subqueries and other relation types
- Caches per-attribute width estimates in rel->attr_widths array for reuse during join planning
- Despite its name, also computes and sets reltarget->cost for expression evaluation
- Handles whole-row references by summing all column widths plus heap tuple header overhead
- Uses statistical information when available, falling back to datatype-based estimates
- All width calculations are clamped to prevent integer overflow using `clamp_width_est`
- Assumes Vars have zero cost while other expressions are properly costed