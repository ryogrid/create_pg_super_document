# fix_join_expr_context

## Location
src/backend/optimizer/plan/setrefs.c: 73 - 82

## Overview
A context structure used during join expression fixing to provide access to outer and inner relation target lists and other necessary information for resolving variable references in join expressions.

## Definition


## Detailed Description
The  structure provides the context needed for fixing expressions at join nodes in PostgreSQL's query planner. This structure contains indexed target lists for both outer and inner relations participating in the join, allowing efficient lookup and replacement of variable references.

During the expression fixing phase, join expressions need access to variables from both sides of the join. This structure provides the necessary indexed access to both outer and inner target lists, along with additional context for proper variable resolution, nulling relation handling, and range table offset adjustments.

The structure is used by expression mutator functions that traverse join condition expressions and other join-related expressions, updating variable references to point to the correct target list entries from the outer and inner relations.

## Parameters / Member Variables
- : Pointer to the PlannerInfo structure containing global planner state
- : Indexed target list for the outer (left) relation in the join
- : Indexed target list for the inner (right) relation in the join
- : Index of the relation whose variables are acceptable in this context
- : Range table offset adjustment for nested query contexts
- : Nulling relations matching mode for handling outer join semantics
- : Estimated number of executions for cost-based decisions

## Dependencies
- Functions called/Symbols referenced:
  - PlannerInfo (planner's main state structure)
  - indexed_tlist (indexed target list structure)
  - NullingRelsMatch (enumeration for nulling relation matching)
- Called from (representative examples):
  - fix_scan_list
  - fix_join_expr
  - fix_join_expr_mutator

## Notes and Other Information
- Used specifically for join-level expression fixing, distinct from scan and upper-level contexts
- The dual target lists (outer_itlist and inner_itlist) enable proper variable resolution for join conditions
- The acceptable_rel field helps enforce variable visibility rules in complex join hierarchies
- The nrm_match field is crucial for handling outer join semantics correctly
- Part of the expression reference fixing framework that ensures proper variable binding in the final plan