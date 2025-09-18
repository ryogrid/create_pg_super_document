# set_pathtarget_cost_width

## Location
src/backend/optimizer/path/costsize.c: 6259 - 6296

## Overview
Sets the estimated evaluation cost and output width for a PathTarget target list, leveraging cached width estimates where available and computing expression evaluation costs for non-Var nodes.

## Definition
```c
PathTarget *set_pathtarget_cost_width(PlannerInfo *root, PathTarget *target)
```

## Detailed Description
This function computes both the evaluation cost and tuple width for a PathTarget, which represents a collection of expressions that need to be computed at some point during query execution. It iterates through all expressions in the target list, accumulating their widths using `get_expr_width` and computing evaluation costs for non-Var expressions using `cost_qual_eval_node`. The function assumes that Vars (simple column references) have zero evaluation cost, while more complex expressions require computational effort that must be accounted for in the cost model.

The function is designed to work efficiently by leveraging width estimates cached during earlier `set_rel_width` calls for base relations. When cached estimates aren't available, it falls back to datatype-based estimates. This approach balances accuracy with performance, as early planning phases don't require catalog lookups for precise width information.

The function serves as a notational convenience by returning the same PathTarget pointer that was passed in, allowing for method chaining in calling code.

## Parameters / Member Variables
- `root`: Pointer to PlannerInfo structure containing global planning information and context
- `target`: Pointer to PathTarget structure whose cost and width are being calculated

## Dependencies
- Functions called/Symbols referenced:
  - get_expr_width
  - cost_qual_eval_node
  - clamp_width_est
- Types used:
  - PathTarget
  - QualCost
- Called from (representative examples):
  - make_group_input_target
  - make_partial_grouping_target
  - make_window_input_target
  - make_sort_input_target
  - split_pathtarget_at_srfs
  - create_pathtarget

## Notes and Other Information
- Returns the same PathTarget pointer passed in for notational convenience (method chaining)
- Assumes Vars have zero evaluation cost while other expressions are properly costed
- Leverages cached width estimates from previous `set_rel_width` calls when available
- Falls back to datatype-based width estimates when cached values are unavailable
- Width calculations are clamped using `clamp_width_est` to prevent overflow
- Designed for efficiency during early planning phases where catalog accuracy isn't critical
- Essential component of PostgreSQL's cost-based query optimization infrastructure