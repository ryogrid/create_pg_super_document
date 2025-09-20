# fix_join_expr_context

## Location
[src/backend/optimizer/plan/setrefs.c:73-82](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L73-L82)

## Overview
A context structure used during join expression fixing to provide access to outer and inner relation target lists and other necessary information for resolving variable references in join expressions.

## Definition

```c
typedef struct
{
	PlannerInfo *root;
	indexed_tlist *subplan_itlist;
	int			newvarno;
	int			rtoffset;
	NullingRelsMatch nrm_match;
	double		num_exec;
} fix_upper_expr_context;
```
## Detailed Description
The  structure provides the context needed for fixing expressions at join nodes in PostgreSQL's query planner. This structure contains indexed target lists for both outer and inner relations participating in the join, allowing efficient lookup and replacement of variable references.

During the expression fixing phase, join expressions need access to variables from both sides of the join. This structure provides the necessary indexed access to both outer and inner target lists, along with additional context for proper variable resolution, nulling relation handling, and range table offset adjustments.

The structure is used by expression mutator functions that traverse join condition expressions and other join-related expressions, updating variable references to point to the correct target list entries from the outer and inner relations.

## Parameters / Member Variables
- `root`: Pointer to the PlannerInfo structure containing global planner state
- `outer_itlist`: Indexed target list for the outer (left) relation in the join
- `inner_itlist`: Indexed target list for the inner (right) relation in the join
- `acceptable_rel`: Index of the relation whose variables are acceptable in this context
- `rtoffset`: Range table offset adjustment for nested query contexts
- `nrm_match`: Nulling relations matching mode for handling outer join semantics
- `num_exec`: Estimated number of executions for cost-based decisions

## Dependencies
- Functions called/Symbols referenced:
  - [PlannerInfo](../P/PlannerInfo.md) (planner's main state structure)
  - [indexed_tlist](../i/indexed_tlist.md) (indexed target list structure)
  - NullingRelsMatch (enumeration for nulling relation matching)
- Called from (representative examples):
  - fix_scan_list
  - [fix_join_expr](fix_join_expr.md)
  - [fix_join_expr_mutator](fix_join_expr_mutator.md)

## Notes and Other Information
- Used specifically for join-level expression fixing, distinct from scan and upper-level contexts
- The dual target lists (outer_itlist and inner_itlist) enable proper variable resolution for join conditions
- The acceptable_rel field helps enforce variable visibility rules in complex join hierarchies
- The nrm_match field is crucial for handling outer join semantics correctly
- Part of the expression reference fixing framework that ensures proper variable binding in the final plan