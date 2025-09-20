# fix_upper_expr_context

## Location
[src/backend/optimizer/plan/setrefs.c:83-89](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L83-L89)

## Overview
A context structure used during upper-level expression fixing to provide access to subplan target lists and variable renumbering information for resolving references in upper query plan nodes.

## Definition

```c
typedef struct
{
	PlannerInfo *root;
	indexed_tlist *subplan_itlist;
	int			newvarno;
} fix_windowagg_cond_context;
```
## Detailed Description
The  structure provides the necessary context for fixing expressions in upper-level plan nodes (such as Agg, Sort, Group, WindowAgg, etc.) in PostgreSQL's query planner. Upper-level nodes typically receive input from a single subplan and need to resolve variable references to point to the appropriate entries in that subplan's target list.

This structure contains an indexed target list from the subplan, along with variable renumbering information that allows the expression fixing process to map variables from their original range table positions to new positions in the upper plan node's context. This is essential for maintaining correct variable references as the query plan is finalized.

The structure is used by expression mutator functions that traverse expressions in upper-level plan nodes, updating variable references to ensure they correctly reference the subplan's output.

## Parameters / Member Variables
- : Pointer to the PlannerInfo structure containing global planner state
- : Indexed target list for the subplan providing input to this upper-level node
- : New variable number to assign to variables in this context
- : Range table offset adjustment for nested query contexts
- : Nulling relations matching mode for handling outer join semantics
- : Estimated number of executions for cost-based optimization decisions

## Dependencies
- Functions called/Symbols referenced:
  - [PlannerInfo](../P/PlannerInfo.md) (planner's main state structure)
  - [indexed_tlist](../i/indexed_tlist.md) (indexed target list structure)
  - NullingRelsMatch (enumeration for nulling relation matching)
- Called from (representative examples):
  - fix_scan_list
  - [fix_upper_expr](fix_upper_expr.md)
  - [fix_upper_expr_mutator](fix_upper_expr_mutator.md)

## Notes and Other Information
- Used for upper-level plan nodes like Agg, Sort, Group, WindowAgg, Limit, etc.
- The newvarno field enables proper variable renumbering for upper-level contexts
- Single subplan_itlist reflects that upper nodes typically have one input source
- Part of the expression reference fixing framework that ensures variables point to correct target list positions
- The structure handles the transition from multi-relation contexts (in joins) to single-relation contexts (in upper nodes)